CREATE TABLE action_types (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50) UNIQUE
);

INSERT INTO action_types (name) VALUES
('first_visit'), ('registration'), ('login'), ('logout'),
('create_topic'), ('view_topic'), ('delete_topic'), ('write_message');

CREATE TABLE user_logs (
    id SERIAL PRIMARY KEY,
    user_id INT,
    action_id INT REFERENCES action_types(id),
    object_id INT,
    status VARCHAR(10),
    created_at TIMESTAMP
);
