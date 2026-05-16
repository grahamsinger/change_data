-- Schema for the CDC demo. Two tables: authors and articles.
-- Debezium will produce one Kafka topic per table.

CREATE DATABASE IF NOT EXISTS demo;
USE demo;

CREATE TABLE IF NOT EXISTS authors (
  id          INT AUTO_INCREMENT PRIMARY KEY,
  name        VARCHAR(120) NOT NULL UNIQUE,
  created_at  TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS articles (
  id            INT AUTO_INCREMENT PRIMARY KEY,
  author_id     INT          NOT NULL,
  title         VARCHAR(255) NOT NULL,
  body          TEXT         NOT NULL,
  kind          ENUM('poem','article') NOT NULL DEFAULT 'article',
  published_at  TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (author_id) REFERENCES authors(id) ON DELETE CASCADE,
  INDEX idx_articles_author (author_id),
  INDEX idx_articles_published (published_at)
) ENGINE=InnoDB;

-- Replication user for Debezium. Needs REPLICATION SLAVE + REPLICATION CLIENT
-- to read binlogs, plus RELOAD for the initial snapshot.
CREATE USER IF NOT EXISTS 'debezium'@'%' IDENTIFIED BY 'dbzpw';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT, LOCK TABLES ON *.* TO 'debezium'@'%';
FLUSH PRIVILEGES;
