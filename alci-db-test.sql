-- ЭТАП 1
DROP TABLE IF EXISTS users;
DROP TABLE IF EXISTS users_audit;

CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name TEXT,
    email TEXT,
    role TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE users_audit (
    id SERIAL PRIMARY KEY,
    user_id INTEGER,
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    changed_by TEXT,
    field_changed TEXT,
    old_value TEXT,
    new_value TEXT
);

-- ЭТАП 2
CREATE OR REPLACE FUNCTION log_user_audit()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    
    IF OLD.name IS DISTINCT FROM NEW.name THEN
        INSERT INTO users_audit(user_id, changed_at, changed_by, field_changed, old_value, new_value)
        VALUES (OLD.id, CURRENT_TIMESTAMP, CURRENT_USER, 'name', OLD.name, NEW.name);
    END IF;
    
    IF OLD.email IS DISTINCT FROM NEW.email THEN
        INSERT INTO users_audit(user_id, changed_at, changed_by, field_changed, old_value, new_value)
        VALUES (OLD.id, CURRENT_TIMESTAMP, CURRENT_USER, 'email', OLD.email, NEW.email);
    END IF;
    
    IF OLD.role IS DISTINCT FROM NEW.role THEN
        INSERT INTO users_audit(user_id, changed_at, changed_by, field_changed, old_value, new_value)
        VALUES (OLD.id, CURRENT_TIMESTAMP, CURRENT_USER, 'role', OLD.role, NEW.role);
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_log_user_audit ON users;
CREATE TRIGGER trigger_log_user_audit
    BEFORE UPDATE ON users
    FOR EACH ROW
    EXECUTE FUNCTION log_user_audit();

-- ЭТАП 3
CREATE EXTENSION IF NOT EXISTS pg_cron;

CREATE OR REPLACE FUNCTION export_audit_to_csv()
RETURNS VOID AS $$
DECLARE
--  yesterday DATE := CURRENT_DATE; - для ручной проверки
    yesterday DATE := CURRENT_DATE - 1;
    filename  TEXT := '/tmp/users_audit_export_' || to_char(yesterday, 'YYYY-MM-DD') || '.csv';
BEGIN
    EXECUTE format('
        COPY (
            SELECT user_id, changed_at, changed_by, field_changed, old_value, new_value
            FROM users_audit
            WHERE DATE(changed_at) = %L
            ORDER BY changed_at
        ) TO %L CSV HEADER
    ', yesterday, filename);
END;
$$ LANGUAGE plpgsql;

SELECT cron.schedule(
    'daily-audit-export',
--  '* * * * *' - для ручной проверки
    '0 3 * * *',
    'SELECT export_audit_to_csv();'
);
/*
-- Тестовые данные
INSERT INTO users (name, email, role) VALUES 
('Nikolai', 'nick@example.com', 'admin'),
('Vera', 'vera@example.com', 'user');
-- SELECT * FROM users;

UPDATE users SET role = 'admin' WHERE name = 'Vera';
UPDATE users SET email = 'nick.new@example.com' WHERE name = 'Nikolai';
-- SELECT * FROM users;
-- SELECT * FROM users_audit;

SELECT export_audit_to_csv();
*/
