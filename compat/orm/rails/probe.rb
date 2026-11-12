require 'active_record'
require 'pg'
require 'json'
require 'logger'
ActiveRecord::Base.logger = Logger.new($stdout)
ActiveRecord::Base.establish_connection(adapter: 'postgresql', database: 'postgres', username: 'postgres',
  password: ENV.fetch('PGPASSWORD'), host: ENV.fetch('PGHOST'), port: ENV.fetch('PGPORT'))
$rows, $sql = [], []
ActiveSupport::Notifications.subscribe('sql.active_record') do |*, payload|
  error = payload[:exception_object]
  cause = error.respond_to?(:cause) ? error.cause : error
  state = cause.respond_to?(:result) && cause.result&.error_field(PG::Result::PG_DIAG_SQLSTATE)
  $sql << {sql: payload[:sql], parameters: payload[:binds].inspect, sqlstate: state || nil}
end
def check(value)
  raise 'Model assertion failed' unless value
end
def step(name, blocked = false)
  $sql.clear
  row = {step: name, status: 'FAIL', classification: blocked ? 'blocked' : nil}
  begin
    raise 'Blocked by forward migration failure; no workaround schema installed.' if blocked
    yield
    row[:status] = 'PASS'
  rescue => e
    cause = e
    cause = cause.cause while cause.cause
    state = cause.respond_to?(:result) && cause.result&.error_field(PG::Result::PG_DIAG_SQLSTATE)
    row.merge!(error: e.message, exception: e.class.name, sqlstate: state || nil,
      failures: $sql.dup, classification: blocked ? 'blocked' : (state ? 'product-gap' : 'needs-review'))
  end
  $rows << row
  row[:status] == 'PASS'
end
class User < ActiveRecord::Base
  has_many :orders
end
class Order < ActiveRecord::Base
  belongs_to :user
  has_many :order_items
end
class OrderItem < ActiveRecord::Base
  self.primary_key = [:order_id, :sku]
  belongs_to :order
end
context = ActiveRecord::MigrationContext.new('/app/migrations')
ok = step('migrate') do
  # Same existing-database connection used by Rails db:create/db:migrate tasks.
  check(ActiveRecord::Base.connection.select_value('SELECT 1') == 1)
  context.migrate
end
step('create_read', !ok) do
  User.create!(id: 1, email: 'ada@example.test')
  Order.create!(id: 10, user_id: 1, status: 'new', total: '12.50', created_at: Time.utc(2026, 1, 2))
  OrderItem.create!(order_id: 10, sku: 'book', quantity: 2)
  order = Order.find(10)
  check(order.total == BigDecimal('12.50') && order.created_at == Time.utc(2026, 1, 2))
end
step('relations', !ok) do
  check(Order.includes(:user, :order_items).find(10).user.email == 'ada@example.test')
  check(OrderItem.includes(:order).find([10, 'book']).order.id == 10)
end
step('update', !ok) { check(Order.where(id: 10).update_all(status: 'paid') == 1 && Order.find(10).status == 'paid') }
step('nested_atomic', !ok) do
  User.transaction do
    User.create!(id: 3, email: 'outer@example.test')
    User.transaction(requires_new: true) do
      User.create!(id: 4, email: 'inner@example.test')
      raise ActiveRecord::Rollback
    end
  end
  check(User.exists?(3) && !User.exists?(4))
end
step('unique_integrity', !ok) do
  begin
    User.create!(id: 2, email: 'ada@example.test')
    raise 'Expected ActiveRecord::RecordNotUnique'
  rescue ActiveRecord::RecordNotUnique => e
    check(e.cause.result.error_field(PG::Result::PG_DIAG_SQLSTATE) == '23505')
  end
end
step('filter_order_page', !ok) { check(User.where('id >= ?', 1).order(:id).limit(1).offset(1).pluck(:id) == [3]) }
step('aggregate_count', !ok) { check(Order.count == 1) }
step('delete', !ok) { OrderItem.find([10, 'book']).destroy!; check(!OrderItem.exists?(order_id: 10, sku: 'book')) }
step('migrate_zero') { context.migrate(0); check((ActiveRecord::Base.connection.tables & %w[users orders order_items]).empty?) }
puts 'ORM_RESULT=' + JSON.generate($rows)
