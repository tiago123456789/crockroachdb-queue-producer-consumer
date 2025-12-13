CREATE TYPE job_status AS ENUM ('pending', 'in_progress');

CREATE TABLE jobs (
    id SERIAL PRIMARY KEY,
    status job_status NOT NULL DEFAULT 'pending', -- Tracks the job lifecycle
    payload JSONB, -- Chose payload as `jsonb` but could have gone with `bytea` instead.
    visible_at TIMESTAMP DEFAULT now(), -- SQS visibility timeout (will come back to this later)
    retry_count INT DEFAULT 0, -- Tracks retry attempts
    created_at TIMESTAMP DEFAULT now(),
    updated_at TIMESTAMP DEFAULT now()
);

CREATE TABLE jobs_dlq (
    id SERIAL PRIMARY KEY,
    status job_status NOT NULL DEFAULT 'pending', -- Tracks the job lifecycle
    payload JSONB, -- Chose payload as `jsonb` but could have gone with `bytea` instead.
    visible_at TIMESTAMP DEFAULT now(), -- SQS visibility timeout (will come back to this later)
    retry_count INT DEFAULT 0, -- Tracks retry attempts
    created_at TIMESTAMP DEFAULT now(),
    updated_at TIMESTAMP DEFAULT now()
);


