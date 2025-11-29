import pyarrow as pa
import pyarrow.parquet as pq

from faker import Faker
from modules.providers import ProductProvider

import random

fake = Faker()
fake.add_provider(ProductProvider)
fake.seed_instance(67)

def generate_customers(num_records: int):
    for i in range(1, num_records + 1):
        name = f"{fake.first_name()} {fake.last_name()}"
        email = fake.email()
        phone = fake.phone_number()
        home_address = fake.address()
        rewards_member = fake.random_choices(elements=[True, False], length=1)[0]

        yield {
            "customer_id": i,
            "name": name,
            "email": email,
            "home_address": home_address,
            "phone": phone,
            "rewards_member": rewards_member
        }

def generate_discounts(num_records: int):
    for i in range(1, num_records + 1):
        name = fake.word().upper()
        percentage = round(fake.random_int(min=5, max=50) / 100, 2)

        yield {
            "discount_id": i,
            "name": name,
            "percentage": percentage
        }


def generate_products(num_records: int):
    for i in range(1, num_records + 1):
        product_info = fake.product()

        yield {
            "product_id": i,
            "name": product_info[0],
            "category": product_info[1], 
            "price": fake.random_number(digits=2),
        }

def generate_orders(num_records: int, customer_num_records: int, product_num_records: int, discount_num_records: int):
    customers = list(generate_customers(customer_num_records))
    products = list(generate_products(product_num_records))
    discounts = list(generate_discounts(discount_num_records))

    customer_ids = [customer['customer_id'] for customer in customers]
    discount_ids = [discount['discount_id'] for discount in discounts]
    product_ids = [product['product_id'] for product in products]

    for i in range(1, num_records + 1):
        customer_id = fake.random_choices(elements=customer_ids, length=1)[0]
        product_id = fake.random_choices(elements=product_ids, length=1)[0]
        discount_id = fake.random_choices(elements=discount_ids, length=1)[0]
        quantity = fake.random_int(min=1, max=12)
        date_ordered = fake.date_between(start_date='-1y', end_date='-1w')
        date_delivered = fake.date_between(start_date='-1y', end_date='today')

        yield {
            "order_id": i,
            "customer_id": customer_id,
            "product_id": product_id,
            "discount_id": discount_id,
            "quantity": quantity,
            "date_ordered": date_ordered,
            "date_delivered": date_delivered
        }

def randomize_null_data(data: list[dict], k: int) -> list[dict]:
    all_keys = list(data[0].keys())
    for record in data:
        keys_to_modify = random.sample(all_keys, k)
        for key in keys_to_modify:
            rng = random.random()
            match key:
                case "customer_id" | "product_id" | "order_id" | "discount_id":
                    continue
                
                case _:
                   if rng < 0.5:
                        record[key] = None

    return data
        

def save_to_parquet(generator, file_name: str, k: int = 1):
    data = randomize_null_data(list(generator), k)
    table = pa.Table.from_pylist(data)
    pq.write_table(table, f"../data/{file_name}.parquet")