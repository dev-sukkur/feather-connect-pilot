#!/bin/sh
# The two test environments' databases (deploy/compose.nonprod.yaml). Postgres runs this once, on an empty volume.
# Each database has its own user, which owns it; neither user can connect to the other's database.
set -eu
psql -v ON_ERROR_STOP=1 -U postgres -d postgres \
  -v qa_password="$QA_DB_PASSWORD" -v uat_password="$UAT_DB_PASSWORD" <<'SQL'
create role feather_qa login password :'qa_password';
create role feather_uat login password :'uat_password';
create database feather_qa owner feather_qa;
create database feather_uat owner feather_uat;
revoke connect on database feather_qa from public;
revoke connect on database feather_uat from public;
grant connect on database feather_qa to feather_qa;
grant connect on database feather_uat to feather_uat;
SQL
