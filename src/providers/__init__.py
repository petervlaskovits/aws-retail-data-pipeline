from faker.providers import BaseProvider

class ProductProvider(BaseProvider):
    product_categories = [
        'Electronics', 
        'Clothing', 
        'Home', 
        'Books', 
        'Toys'
    ]

    product_names = [
        'Laptop', 
        'Smartphone', 
        'Jeans', 
        'T-Shirt', 
        'Lamp', 
        'Blender', 
        'Comic Book',
        'Non-fiction Book', 
        'Action Figure', 
        'Board Game'
    ]
    
    def product(self):
        category = self.random_element(self.product_categories)
        match category:
            case 'Electronics':
                return (category, self.random_element(self.product_names[:2]))
            case 'Clothing':
                return (category, self.random_element(self.product_names[2:4]))
            case 'Home':
                return (category, self.random_element(self.product_names[4:6]))
            case 'Books':
                return (category, self.random_element(self.product_names[6:8]))
            case 'Toys':
                return (category, self.random_element(self.product_names[8:10]))
                