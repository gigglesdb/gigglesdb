"""One unchanged sample workload for either PostgreSQL-compatible target."""
import datetime as dt
import json
import os
import sys
from decimal import Decimal
import psycopg

sys.modules.setdefault('probe', sys.modules[__name__])
rows, failures = [], []
execute = psycopg.Cursor.execute


def traced(self, query, params=None, *args, **kwargs):
    try:
        return execute(self, query, params, *args, **kwargs)
    except Exception as exc:
        sql = query.as_string(self.connection) if hasattr(query, 'as_string') else str(query)
        failures.append(dict(sql=sql, parameters=repr(params), sqlstate=getattr(exc, 'sqlstate', None), error=str(exc)))
        raise


psycopg.Cursor.execute = traced


def step(name, action, blocked=False):
    failures.clear()
    row = dict(step=name, status='FAIL', classification='blocked' if blocked else None)
    if blocked:
        row['error'] = 'Blocked by forward migration failure; no workaround schema installed.'
    else:
        try:
            action()
            row['status'] = 'PASS'
        except Exception as exc:
            cause = exc
            while cause.__cause__ is not None:
                cause = cause.__cause__
            state = getattr(cause, 'sqlstate', None) or next((f['sqlstate'] for f in reversed(failures) if f['sqlstate']), None)
            row.update(error=str(exc), exception=type(exc).__name__, sqlstate=state,
                       failures=list(failures), classification='product-gap' if state else 'needs-review')
    rows.append(row)
    return row['status'] == 'PASS'


def check(value):
    assert value


def django_run():
    from django.conf import settings
    settings.configure(INSTALLED_APPS=['shop'], USE_TZ=True, DATABASES={'default': dict(
        ENGINE='django.db.backends.postgresql', NAME='postgres', USER='postgres',
        PASSWORD=os.environ['PGPASSWORD'], HOST=os.environ['PGHOST'], PORT=os.environ['PGPORT'])})
    import django
    django.setup()
    from django.core.management import call_command
    from django.db import IntegrityError, transaction
    from django.db.models import Count
    from shop.models import User, Order, OrderItem
    ok = step('migrate', lambda: call_command('migrate', verbosity=0))

    def create():
        User.objects.create(id=1, email='ada@example.test')
        Order.objects.create(id=10, user_id=1, status='new', total=Decimal('12.50'), created_at=dt.datetime(2026, 1, 2, tzinfo=dt.timezone.utc))
        OrderItem.objects.create(order_id=10, sku='book', quantity=2)
        order = Order.objects.get(pk=10)
        check(order.total == Decimal('12.50') and order.created_at == dt.datetime(2026, 1, 2, tzinfo=dt.timezone.utc))

    def relations():
        check(Order.objects.select_related('user').get(pk=10).user.email == 'ada@example.test')
        check(OrderItem.objects.select_related('order').get(pk=(10, 'book')).order.id == 10)

    def unique():
        try:
            with transaction.atomic():
                User.objects.create(id=2, email='ada@example.test')
        except IntegrityError as exc:
            check(exc.__cause__.sqlstate == '23505')
        else:
            raise AssertionError('Expected IntegrityError')

    def atomic():
        with transaction.atomic():
            User.objects.create(id=3, email='outer@example.test')
            try:
                with transaction.atomic():
                    User.objects.create(id=4, email='inner@example.test')
                    raise ValueError('rollback nested savepoint')
            except ValueError:
                pass
        check(User.objects.filter(id=3).exists() and not User.objects.filter(id=4).exists())

    def delete():
        check(OrderItem.objects.filter(pk=(10, 'book')).delete()[0] == 1)
        check(not OrderItem.objects.filter(pk=(10, 'book')).exists())

    actions = [('create_read', create), ('relations', relations),
               ('update', lambda: check(Order.objects.filter(pk=10).update(status='paid') == 1 and Order.objects.get(pk=10).status == 'paid')),
               ('nested_atomic', atomic), ('unique_integrity', unique),
               ('filter_order_page', lambda: check(list(User.objects.filter(id__gte=1).order_by('id')[1:2].values_list('id', flat=True)) == [3])),
               ('aggregate_count', lambda: check(Order.objects.aggregate(n=Count('id')) == {'n': 1})), ('delete', delete)]
    for name, action in actions:
        step(name, action, blocked=not ok)
    step('migrate_zero', lambda: call_command('migrate', 'shop', 'zero', verbosity=0))
    if not ok:
        rows[-1]['note'] = 'Forward migration failed; reverse command does not qualify a successful round-trip.'


def sqlalchemy_run():
    global engine
    from sqlalchemy import create_engine, select, func
    from sqlalchemy.engine import URL
    from sqlalchemy.orm import Session, joinedload
    from sqlalchemy.exc import IntegrityError
    from alembic import command
    from alembic.config import Config
    from sa_models import User, Order, OrderItem
    engine = create_engine(URL.create('postgresql+psycopg', username='postgres', password=os.environ['PGPASSWORD'],
                           host=os.environ['PGHOST'], port=int(os.environ['PGPORT']), database='postgres'))
    cfg = Config()
    cfg.set_main_option('script_location', '/app/alembic')
    ok = step('upgrade_head', lambda: command.upgrade(cfg, 'head'))

    def create(s):
        user = User(id=1, email='ada@example.test')
        order = Order(id=10, user=user, status='new', total=Decimal('12.50'), created_at=dt.datetime(2026, 1, 2, tzinfo=dt.timezone.utc))
        s.add_all([user, order, OrderItem(order=order, sku='book', quantity=2)])
        s.flush()
        s.expire_all()
        order = s.get(Order, 10)
        check(order.total == Decimal('12.50') and order.created_at == dt.datetime(2026, 1, 2, tzinfo=dt.timezone.utc))

    def relations(s):
        check(s.scalar(select(Order).options(joinedload(Order.user))).user.email == 'ada@example.test')
        check(s.get(OrderItem, (10, 'book')).order.id == 10)

    def update(s):
        s.get(Order, 10).status = 'paid'
        s.flush()
        s.expire_all()
        check(s.get(Order, 10).status == 'paid')

    def atomic(s):
        s.add(User(id=3, email='outer@example.test'))
        s.flush()
        try:
            with s.begin_nested():
                s.add(User(id=4, email='inner@example.test'))
                s.flush()
                raise ValueError('rollback nested savepoint')
        except ValueError:
            pass
        check(s.get(User, 3) is not None and s.get(User, 4) is None)

    def unique(s):
        try:
            with s.begin_nested():
                s.add(User(id=2, email='ada@example.test'))
                s.flush()
        except IntegrityError as exc:
            check(exc.orig.sqlstate == '23505')
        else:
            raise AssertionError('Expected IntegrityError')

    def delete(s):
        s.delete(s.get(OrderItem, (10, 'book')))
        s.flush()
        check(s.get(OrderItem, (10, 'book')) is None)

    def session(action):
        with Session(engine) as s, s.begin():
            action(s)

    actions = [('create_read', create), ('relations', relations), ('update', update), ('nested_atomic', atomic),
               ('unique_integrity', unique), ('filter_order_page', lambda s: check(list(s.scalars(select(User.id).where(User.id >= 1).order_by(User.id).offset(1).limit(1))) == [3])),
               ('aggregate_count', lambda s: check(s.scalar(select(func.count(Order.id))) == 1)), ('delete', delete)]
    for name, action in actions:
        step(name, lambda action=action: session(action), blocked=not ok)
    step('downgrade_base', lambda: command.downgrade(cfg, 'base'))
    engine.dispose()


if __name__ == '__main__':
    try:
        (django_run if sys.argv[1] == 'django' else sqlalchemy_run)()
    except Exception as exc:
        rows.append(dict(step='initialization', status='FAIL', classification='harness-issue', error=str(exc)))
    print('ORM_RESULT=' + json.dumps(rows))
