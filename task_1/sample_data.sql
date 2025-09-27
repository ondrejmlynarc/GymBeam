INSERT INTO categories VALUES (1, 'Supplements', NULL);
INSERT INTO categories VALUES (2, 'Vitamins', 1);

INSERT INTO products VALUES (1, 'Whey Protein', 50.0, 'Vanilla flavor', TRUE, 1);
INSERT INTO products VALUES (2, 'Vitamin C', 20.0, '1000mg tablets', TRUE, 2);

INSERT INTO customers VALUES (1, 'Jan', 'Mrkvicka', 'jan.mrkvicka@email.com', '123 Main St', 'Bratislava', '2024-01-15');

INSERT INTO orders VALUES (1, 1, '2024-03-01', 'completed');

INSERT INTO order_items VALUES (1, 1, 1, 2, 50.0);
INSERT INTO order_items VALUES (2, 1, 2, 1, 20.0);

INSERT INTO transactions VALUES (1, 1, '2024-03-01', 'credit_card', 120.0);
