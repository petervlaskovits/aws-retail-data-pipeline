import pandas as pd
from faker import Faker
from providers import ProductProvider

fake = Faker()
fake.add_provider(ProductProvider)
fake.seed_instance(67)

def generate_customer_data(num_records: int):
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

def generate_orders(num_records: int, customers, products, discounts):
    customer_ids = [customer.customer_id for customer in customers]
    product_ids = [product.product_id for product in products]
    discount_ids = [discount.discount_id for discount in discounts]

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

def save_to_parquet(generator, file_name: str):
    df = pd.DataFrame(generator)
    df.to_parquet(f"../data/{file_name}", index=False)

save_to_parquet(generate_customer_data(10), 'customers.parquet')