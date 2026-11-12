class Shop < ActiveRecord::Migration[8.1]
  def change
    create_table(:users, id: false) { |t| t.integer :id, primary_key: true; t.string :email, limit: 120, null: false }
    add_index :users, :email, unique: true
    create_table :orders, id: false do |t|
      t.integer :id, primary_key: true
      t.integer :user_id, null: false
      t.string :status, limit: 20, null: false
      t.decimal :total, precision: 12, scale: 2, null: false
      t.timestamptz :created_at, null: false
    end
    add_foreign_key :orders, :users
    add_index :orders, :user_id
    add_index :orders, :status
    create_table :order_items, primary_key: [:order_id, :sku] do |t|
      t.integer :order_id, null: false
      t.string :sku, limit: 40, null: false
      t.integer :quantity, null: false
    end
    add_foreign_key :order_items, :orders
    add_index :order_items, :order_id
  end
end
