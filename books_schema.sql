-- TODO 1: Створення основної таблиці books
CREATE TABLE books (
    book_id SERIAL PRIMARY KEY,
    title VARCHAR(500) NOT NULL,
    price DECIMAL(10, 2),
    genre VARCHAR(100),
    stock_quantity INTEGER DEFAULT 0,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- TODO 2: Створення таблиці для оброблених даних books_processed
CREATE TABLE books_processed (
    processed_id SERIAL PRIMARY KEY,
    book_id INTEGER,
    title VARCHAR(500),
    original_price DECIMAL(10, 2),
    rounded_price DECIMAL(10, 1),
    genre VARCHAR(100),
    price_category VARCHAR(50),
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- TODO 3: Створення індексів для оптимізації
CREATE INDEX idx_books_genre ON books(genre);
CREATE INDEX idx_books_last_updated ON books(last_updated);
CREATE INDEX idx_books_price_range ON books(price);

-- TODO 4: Додавання тестових даних
INSERT INTO books (title, price, genre, stock_quantity, last_updated) VALUES
('Назва книги 1', 299.99, 'фантастика', 15, '2025-01-15 10:30:00'),
('Назва книги 2', 450.50, 'детектив', 8, '2025-01-10 14:20:00'),
('Назва книги 3', 220.00, 'фантастика', 25, '2025-01-12 09:15:00'),
('Назва книги 4', 750.75, 'історичний роман', 5, '2025-01-08 16:45:00'),
('Назва книги 5', 380.00, 'детектив', 12, '2025-01-14 11:00:00'),
('Назва книги 6', 650.25, 'історичний роман', 3, '2025-01-11 13:30:00');