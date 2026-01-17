## simple relational database management system

### Java RDBMS – Quick Start

This project is a simple relational database engine written in Java with:

- a SQL REPL

- a Spring Boot web interface

- disk persistence

### Requirements

- Java 17+

- Maven
  
### How to Run
  Build the project
  `mvn clean package`

Run the Web App
`mvn spring-boot:run`


Open in your browser:

http://localhost:2026

#### SQL REPL Mode

The REPL is started by running RdbmsApp under the db package.

From your IDE:

Navigate to

`src/main/java/com/jg/rdms/db/RdbmsApp.java`


Run `RdbmsApp`

You will see:

rdms>

### Basic SQL Operations
Insert
- `INSERT INTO users (name) VALUES ('Jane Doe');`
- `INSERT INTO users (name) VALUES ('John Smith');`

Select
- `SELECT * FROM users;`
- `SELECT * FROM users WHERE id = 1;`

Update
- `UPDATE users SET name = 'Jane Updated' WHERE id = 1;`

Delete
- `DELETE FROM users WHERE id = 1;`

### Web Interface

The web UI provides basic CRUD operations for users:

- View all users

- Add a new user

- Edit an existing user

- Delete a user

All operations are executed against the custom database engine (no JPA / no JDBC).

### Data Persistence

- Data is stored in the data/ directory

- Tables and schema persist across restarts

- You can reset the database by deleting the data/ folder

`rm -rf data/`