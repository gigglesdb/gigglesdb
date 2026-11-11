export interface User {
  id: string;
  email: string;
  name: string | null;
  version: number;
  created_at: Date;
  active: boolean;
}
export interface Account {
  id: string;
  balance: string;
}
export interface Product {
  id: string;
  price: string;
}
export interface Order {
  id: string;
  total: string;
  calculated: string;
  rank: string;
}
