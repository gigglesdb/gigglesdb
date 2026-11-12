from django.db import models


class User(models.Model):
    id = models.IntegerField(primary_key=True)
    email = models.CharField(max_length=120, unique=True)


class Order(models.Model):
    id = models.IntegerField(primary_key=True)
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    status = models.CharField(max_length=20, db_index=True)
    total = models.DecimalField(max_digits=12, decimal_places=2)
    created_at = models.DateTimeField()


class OrderItem(models.Model):
    pk = models.CompositePrimaryKey('order_id', 'sku')
    order = models.ForeignKey(Order, on_delete=models.CASCADE)
    sku = models.CharField(max_length=40)
    quantity = models.IntegerField()
