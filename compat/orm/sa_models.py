from sqlalchemy import Column, DateTime, ForeignKey, Integer, Numeric, String
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


class User(Base):
    __tablename__ = 'shop_user'
    id = Column(Integer, primary_key=True, autoincrement=False)
    email = Column(String(120), unique=True, nullable=False)


class Order(Base):
    __tablename__ = 'shop_order'
    id = Column(Integer, primary_key=True, autoincrement=False)
    user_id = Column(Integer, ForeignKey('shop_user.id'), nullable=False, index=True)
    status = Column(String(20), nullable=False, index=True)
    total = Column(Numeric(12, 2), nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False)
    user = relationship(User)


class OrderItem(Base):
    __tablename__ = 'shop_orderitem'
    order_id = Column(Integer, ForeignKey('shop_order.id'), primary_key=True, index=True)
    sku = Column(String(40), primary_key=True)
    quantity = Column(Integer, nullable=False)
    order = relationship(Order)
