-- Role search_path for the TSIGMA database.
--
-- Tables live in four logical schemas (config, events, aggregation, identity),
-- but the audit-trigger functions and the user_role enum resolve UNQUALIFIED at
-- runtime, and the initial migration's foreign keys resolve through the role's
-- search_path rather than being schema-qualified. Without this, the very first
-- migration fails with: relation "region" does not exist.
--
-- Mounted into /docker-entrypoint-initdb.d/ by docker-compose.yml. Postgres runs
-- these scripts ONLY when initialising an empty data directory, so an existing
-- volume is unaffected -- run the statement by hand there:
--
--   docker compose exec db psql -U tsigma -d tsigma \
--     -c "ALTER ROLE tsigma SET search_path = config, events, aggregation, identity, public;"
--
-- Non-container deployments set the same thing; see docs/users/DEPLOYMENT.md.

ALTER ROLE tsigma SET search_path = config, events, aggregation, identity, public;
