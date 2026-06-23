import psycopg2
import time

def connection():
    for _ in range(10):
        try:
            return psycopg2.connect(
                host="db",
                database="inventory_db",
                user="admin",
                password="admin123"
            )
            break

        except psycopg2.OperationalError:
            time.sleep(5)

def add_product(name, price, stock):
    conn = connection()
    cur = conn.cursor()

    query = """
        INSERT INTO products (name, price, stock)
        VALUES (%s, %s, %s)
    """

    cur.execute(query, (name, price, stock))

    conn.commit()

    cur.close()
    conn.close()

    print("Product added.")

def display_products():
    conn = connection()
    cur = conn.cursor()

    cur.execute("SELECT * FROM products")
    products = cur.fetchall()
    if not products:
        print("No products")
    else:
        for product in products:
            print(
                f"""{product[0]} - {product[1]},
                price {product[2]}, stock {product[3]}"""
            )
    cur.close()
    conn.close()

def update_product(id, new_stock_value):
    conn = connection()
    cur = conn.cursor()

    query = """
        UPDATE products
        SET stock = %s
        WHERE id = %s
    """
    cur.execute(query, (new_stock_value, id))
 
    cur.close()
    conn.close()

def delete_product(id):
    conn = connection()
    cur = conn.cursor()

    query = """
        DELETE FROM products
        WHERE id = %s
    """
    cur.execute(query, (id,))
 
    cur.close()
    conn.close()

def main():

    while True:
        print("===== INVENTORY MANAGEMENT ====")
        print("1. Add a product")
        print("2. Display products")
        print("3. Update stock")
        print("4. Delete a product")
        print("5. Exit")

        choice = input("Your choice")

        if choice=="1":
            name = input("Product name:")
            price = float(input("Price:"))
            stock = int(input("Stock:"))
            add_product(name, price, stock)
        elif choice=="2":
            display_products()
        elif choice=="5":
            print("Exit")
            break
        else:
            print("Try again")
            
    if __name__ == "__main__":
        main()
