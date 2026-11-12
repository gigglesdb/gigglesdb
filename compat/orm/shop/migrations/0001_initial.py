from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):
    initial = True
    dependencies = []
    operations = [
        migrations.CreateModel(name='User', fields=[
            ('id', models.IntegerField(primary_key=True, serialize=False)),
            ('email', models.CharField(max_length=120, unique=True))]),
        migrations.CreateModel(name='Order', fields=[
            ('id', models.IntegerField(primary_key=True, serialize=False)),
            ('status', models.CharField(max_length=20, db_index=True)),
            ('total', models.DecimalField(max_digits=12, decimal_places=2)),
            ('created_at', models.DateTimeField()),
            ('user', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='shop.user'))]),
        migrations.CreateModel(name='OrderItem', fields=[
            ('pk', models.CompositePrimaryKey('order_id', 'sku', blank=True, editable=False, primary_key=True, serialize=False)),
            ('sku', models.CharField(max_length=40)),
            ('quantity', models.IntegerField()),
            ('order', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='shop.order'))]),
    ]
