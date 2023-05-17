+---------------+
                                |     User      |
                                +---------------+
                                | user_id       |
                                | name          |
                                | email         |
                                | password      |
                                | address       |
                                | phone_number  |
                                +---------------+
                                          |
                                          |
                        +-----------------+----------------+
                        |                                    |
          +-------------+--------------+       +-------------+--------------+
          |   Product                  |       |   Order                    |
          +---------------------------+       +---------------------------+
          | product_id                |       | order_id                  |
          | seller_id                 |       | buyer_id                  |
          | name                      |       | date_ordered              |
          | description               |       | status                    |
          | price                     |       | shipping_address          |
          | quantity_available        |       | payment_method            |
          +---------------------------+       +---------------------------+
                                          |
                                          |
                                +---------+----------+
                                |   Order Item      |
                                +-------------------+
                                | order_item_id     |
                                | order_id          |
                                | product_id        |
                                | quantity_ordered  |
                                | item_price        |
                                +-------------------+


This ERD includes the following entities:

User: Represents a user of the marketplace, with attributes such as user_id, name, email, password, address, and phone_number.
Product: Represents a product for sale on the marketplace, with attributes such as product_id, seller_id, name, description, price, and quantity_available.
Order: Represents an order placed by a user, with attributes such as order_id, buyer_id, date_ordered, status, shipping_address, and payment_method.
Order Item: Represents a specific item in an order, with attributes such as order_item_id, order_id, product_id, quantity_ordered, and item_price.