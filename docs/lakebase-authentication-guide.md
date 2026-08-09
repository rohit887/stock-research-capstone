# Lakebase Authentication Guide

> **Key Learning from Phase 0 Setup**: Understanding when and why to use native password authentication vs. OAuth for Databricks Lakebase Postgres Autoscaling.

---

## Table of Contents

1. [The Problem We Encountered](#the-problem-we-encountered)
2. [Understanding Lakebase Autoscaling Architecture](#understanding-lakebase-autoscaling-architecture)
3. [Authentication Methods](#authentication-methods)
4. [When to Use Which Auth Method](#when-to-use-which-auth-method)
5. [Databricks Secrets Management](#databricks-secrets-management)
6. [Postgres Roles Explained](#postgres-roles-explained)
7. [Connection String Anatomy](#connection-string-anatomy)
8. [Setup Workflow](#setup-workflow)
9. [Best Practices](#best-practices)

---

## The Problem We Encountered

### Initial Symptoms
- Created tables with `CREATE TABLE` statements
- Connection confirmed successful
- Immediately querying the same connection: **tables don't exist**
- Re-running the same CREATE statements: **still no tables**

### Root Cause
**Lakebase Autoscaling with OAuth tokens routes connections to different backend Postgres instances.**

```
Connection 1 (OAuth token ABC) → Instance A → CREATE TABLE companies
Connection 2 (OAuth token DEF) → Instance B → SELECT * FROM companies ❌ (table not found)
```

**Why?**
- OAuth tokens don't guarantee instance affinity
- DDL changes don't replicate immediately across instances
- Each connection may hit a different backend instance
- Schema changes made on Instance A aren't visible on Instance B

### The Solution
**Native password authentication provides session affinity to a consistent instance.**

```
Connection 1 (student:password) → Instance A → CREATE TABLE companies
Connection 2 (student:password) → Instance A → SELECT * FROM companies ✅ (table found)
```

---

## Understanding Lakebase Autoscaling Architecture

### Single Endpoint, Multiple Instances

```
┌─────────────────────────────────────────────────────────────┐
│ Lakebase Endpoint (DNS)                                     │
│ ep-withered-union-d8dfuwlx.database.us-east-2.cloud....    │
└────────────────────┬────────────────────────────────────────┘
                     │ Load Balancer
         ┌───────────┼───────────┐
         │           │           │
    ┌────▼───┐  ┌────▼───┐  ┌────▼───┐
    │Instance│  │Instance│  │Instance│
    │   A    │  │   B    │  │   C    │
    └────────┘  └────────┘  └────────┘
     Postgres    Postgres    Postgres
       17.10       17.10       17.10
```

### OAuth Token Routing (Stateless)
- Each token is independent
- Load balancer distributes connections
- No guarantee of hitting the same instance
- **Problem for DDL**: Schema changes on Instance A aren't immediately on Instance B

### Native Password Routing (Session Affinity)
- Password-based authentication establishes session affinity
- Subsequent connections with same credentials route to same instance
- **Solution for DDL**: All schema operations hit the same instance

---

## Authentication Methods

### 1. OAuth Token Authentication (Workspace Identity)

**How it works:**
```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
cred = w.postgres.generate_database_credential(endpoint=endpoint_name)

conn = psycopg2.connect(
    host=PG_HOST,
    user=w.current_user.me().user_name,  # Your Databricks email
    password=cred.token,                  # Short-lived JWT token (1 hour)
    database=PG_DATABASE
)
```

**Characteristics:**
- Uses your Databricks workspace identity
- Generates short-lived JWT tokens (~1 hour expiration)
- No password storage needed
- **Routes to any available instance** (stateless)

**Best for:**
- Ad-hoc SELECT queries
- Read-only operations
- Exploratory data analysis
- When schema is already established

**⚠️ NOT recommended for:**
- DDL operations (CREATE, ALTER, DROP)
- Multi-statement transactions
- Operations requiring schema visibility across connections

---

### 2. Native Password Authentication (Database Role)

**How it works:**
```python
# Password stored in Databricks secrets
password = dbutils.secrets.get("lakebase", "password")

conn = psycopg2.connect(
    host=PG_HOST,
    user="student",      # Postgres role name
    password=password,    # Persistent password
    database=PG_DATABASE
)
```

**Characteristics:**
- Uses Postgres native role + password
- Password persistent (doesn't expire)
- **Provides session affinity** to consistent instance
- Requires native auth enabled on Lakebase project

**Best for:**
- DDL operations (CREATE TABLE, ALTER, DROP)
- Schema migrations
- Multi-statement transactions
- Application connections (long-lived)
- Data ingestion pipelines

**✅ Recommended for:**
- Any schema setup or modification
- Production workloads
- Reliable, consistent connections

---

## When to Use Which Auth Method

| Scenario | Auth Method | Reason |
|----------|-------------|--------|
| **Creating tables** | ✅ Native Password | DDL needs consistent instance |
| **Altering schema** | ✅ Native Password | Schema changes must persist |
| **Data ingestion (INSERT/COPY)** | ✅ Native Password | Multi-statement transactions |
| **Ad-hoc SELECT queries** | ⚡ OAuth OK | Stateless, read-only |
| **Exploratory analysis** | ⚡ OAuth OK | No schema changes |
| **Application backend** | ✅ Native Password | Long-lived, stable connection |
| **CI/CD pipelines** | ✅ Native Password | Reliable DDL execution |
| **Notebook prototyping** | ⚡ OAuth OK | Quick testing, no DDL |

### Decision Tree

```
Are you modifying the schema (DDL)?
├─ YES → Use Native Password Authentication ✅
│        (CREATE, ALTER, DROP, GRANT)
│
└─ NO → Is this a long-lived connection or transaction?
    ├─ YES → Use Native Password Authentication ✅
    │        (Application server, ETL pipeline)
    │
    └─ NO → OAuth is acceptable ⚡
             (Quick SELECT, exploratory query)
```

---

## Databricks Secrets Management

### What Are Databricks Secrets?

Secrets provide secure storage for sensitive credentials:
- Passwords
- API keys
- Access tokens
- Connection strings

### Where Are They Stored?

**Location:**
- **Databricks control plane** (workspace-level storage)
- Encrypted at rest and in transit
- Managed by Databricks backend
- **NOT in your notebooks or Git repos**

**Structure:**
```
Workspace
└── Secrets
    ├── Scope: "lakebase"
    │   └── Key: "password" → Value: "<stored-in-secret: lakebase/password>"
    ├── Scope: "capstone"
    │   └── Key: "massive_api_key" → Value: "..."
    └── Scope: "production"
        ├── Key: "db_password" → Value: "..."
        └── Key: "api_token" → Value: "..."
```

### Creating and Using Secrets

**1. Create a scope:**
```bash
databricks secrets create-scope lakebase
```

**2. Store a secret:**
```bash
databricks secrets put-secret lakebase password
# Opens editor to paste the password value
```

**Or via Python SDK:**
```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
w.secrets.create_scope(scope="lakebase")
w.secrets.put_secret(
    scope="lakebase",
    key="password",
    string_value="<stored-in-secret: lakebase/password>"
)
```

**3. Retrieve in notebook:**
```python
password = dbutils.secrets.get("lakebase", "password")
# Returns the secret value (automatically redacted in output)
```

### Security Features

**Auto-redaction:**
```python
password = dbutils.secrets.get("lakebase", "password")
print(f"Password: {password}")  
# Output: Password: [REDACTED]
```

**Access control:**
- Workspace admins can grant/revoke access to scopes
- Scope-level permissions (READ, WRITE, MANAGE)
- Audit logging of secret access

**Best practices:**
- Never hardcode credentials in notebooks
- Store all sensitive values in secrets
- Use descriptive scope names
- Rotate secrets regularly
- Grant minimal necessary permissions

---

## Postgres Roles Explained

### What Is a Postgres Role?

A **role** is Postgres's unified concept for:
1. **Authentication** (who can log in)
2. **Authorization** (what they can do)

Think: Role = User + Permissions

### Why Create a Separate Role?

**Your Databricks identity vs. Database identity:**

```
┌──────────────────────────────────────────┐
│ Databricks Workspace                     │
│                                          │
│ User: moneybridge101@gmail.com          │
│ ├─ OAuth token (1 hour expiration)      │
│ └─ Workspace permissions                │
└──────────────────────────────────────────┘
              ↓ Can generate
┌──────────────────────────────────────────┐
│ Lakebase Postgres Database               │
│                                          │
│ Role: student                            │
│ ├─ Native password (persistent)          │
│ └─ Database permissions (GRANT)          │
└──────────────────────────────────────────┘
```

### Creating a Role with Permissions

**Step 1: Create the role**
```sql
CREATE ROLE student WITH LOGIN PASSWORD '<stored-in-secret: lakebase/password>';
```

**Breakdown:**
- `ROLE student` → Creates a new role named "student"
- `WITH LOGIN` → Allows this role to authenticate (connect)
- `PASSWORD '...'` → Sets the native password for authentication

**Step 2: Grant permissions**
```sql
-- Allow access to the database
GRANT ALL PRIVILEGES ON DATABASE databricks_postgres TO student;

-- Allow creating objects in the database
GRANT CREATE ON DATABASE databricks_postgres TO student;

-- Allow full access to the public schema
GRANT ALL PRIVILEGES ON SCHEMA public TO student;
```

**Privilege types:**
- `ALL PRIVILEGES` → Full access (CREATE, SELECT, INSERT, UPDATE, DELETE)
- `CREATE` → Create new objects (tables, functions)
- `SELECT` → Read data
- `INSERT` → Add rows
- `UPDATE` → Modify rows
- `DELETE` → Remove rows

### Role Benefits

**1. Persistence:**
- Password doesn't expire (unlike OAuth tokens)
- Stable identity across sessions

**2. Separation:**
- Workspace identity ≠ Database identity
- Can have different permissions in each

**3. Sharing:**
- Multiple team members can use the same database role
- Easier permission management (grant to role, not individuals)

**4. Portability:**
- Standard Postgres authentication
- Works with any Postgres client (psql, DBeaver, pgAdmin)

---

## Connection String Anatomy

### Standard Postgres Connection String

```
postgresql://[user]:[password]@[host]:[port]/[database]?[parameters]
```

### Your Working Connection String

```
postgresql://student:<stored-in-secret: lakebase/password>@ep-withered-union-d8dfuwlx.database.us-east-2.cloud.databricks.com:5432/databricks_postgres?sslmode=require
```

### Component Breakdown

| Component | Value | Description |
|-----------|-------|-------------|
| **Protocol** | `postgresql://` | Standard Postgres connection protocol |
| **User** | `student` | The Postgres role name (authentication identity) |
| **Password** | `<stored-in-secret: lakebase/password>` | The role's password (from secrets: `lakebase/password`) |
| **Host** | `ep-withered-union-d8dfuwlx...` | Lakebase endpoint DNS (auto-derived from SDK) |
| **Port** | `5432` | Standard Postgres port |
| **Database** | `databricks_postgres` | The database name within the instance |
| **SSL Mode** | `sslmode=require` | Enforce encrypted connection (mandatory for Lakebase) |

### Python Equivalent

**Using psycopg2:**
```python
import psycopg2

# Connection string format
connection_string = "postgresql://student:<stored-in-secret: lakebase/password>@ep-withered-union-d8dfuwlx.database.us-east-2.cloud.databricks.com:5432/databricks_postgres?sslmode=require"
conn = psycopg2.connect(connection_string)

# OR parameter format (preferred for secrets):
password = dbutils.secrets.get("lakebase", "password")
conn = psycopg2.connect(
    host="ep-withered-union-d8dfuwlx.database.us-east-2.cloud.databricks.com",
    port=5432,
    database="databricks_postgres",
    user="student",
    password=password,  # Retrieved from secrets
    sslmode="require"
)
```

### Security Note

**❌ Never hardcode the password in the connection string:**
```python
# BAD - password visible in code
conn_string = "postgresql://student:<stored-in-secret: lakebase/password>@..."
```

**✅ Always retrieve from secrets:**
```python
# GOOD - password from secure storage
password = dbutils.secrets.get("lakebase", "password")
conn = psycopg2.connect(
    host=PG_HOST,
    user="student",
    password=password,  # Not hardcoded
    ...
)
```

---

## Setup Workflow

### Complete Phase 0 Checklist

#### 1. Enable Native Password Authentication

**Via Databricks CLI:**
```bash
databricks lakebase update-project \
  --project-name stock-research-capstone \
  --enable-native-auth
```

**Via Lakebase UI:**
1. Go to Lakebase in Databricks workspace
2. Open your project: `stock-research-capstone`
3. Click **Settings** → **Authentication**
4. Enable **"Native Password Authentication"**
5. Save changes
6. **Restart the endpoint** for changes to take effect

#### 2. Create the Postgres Role

**Connect with your admin account (OAuth):**
```bash
psql -h ep-withered-union-d8dfuwlx.database.us-east-2.cloud.databricks.com \
     -U moneybridge101@gmail.com \
     -d databricks_postgres
```

**Create the role:**
```sql
CREATE ROLE student WITH LOGIN PASSWORD '<stored-in-secret: lakebase/password>';
GRANT ALL PRIVILEGES ON DATABASE databricks_postgres TO student;
GRANT CREATE ON DATABASE databricks_postgres TO student;
GRANT ALL PRIVILEGES ON SCHEMA public TO student;
```

#### 3. Store Password in Databricks Secrets

**Via CLI:**
```bash
databricks secrets create-scope lakebase
databricks secrets put-secret lakebase password
# Paste: <stored-in-secret: lakebase/password>
```

**Via Python SDK (in notebook):**
```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
w.secrets.create_scope(scope="lakebase")
w.secrets.put_secret(
    scope="lakebase",
    key="password",
    string_value="<stored-in-secret: lakebase/password>"
)
```

#### 4. Configure Notebook Connection

**Update connection function in notebook:**
```python
def _lakebase_password(endpoint_name: str) -> str:
    """Get Postgres password - native password from secrets preferred.
    
    Returns:
        Native password from secrets for persistent connection,
        or OAuth token as fallback.
    """
    # Try native password first (best for DDL operations)
    try:
        native_pw = dbutils.secrets.get("lakebase", "password")
        if native_pw:
            print("[creds] Using native Postgres password from secrets")
            return native_pw
    except Exception:
        pass  # Secret doesn't exist, try OAuth
    
    # Fallback to OAuth token
    cred = w.postgres.generate_database_credential(endpoint=endpoint_name)
    print("[creds] WARNING: OAuth may connect to different instances")
    return cred.token

# Use 'student' role (not email)
PG_USER = "student"

def get_connection():
    return psycopg2.connect(
        host=PG_HOST,
        database=PG_DATABASE,
        user=PG_USER,  # 'student' role
        password=_lakebase_password(primary.name),
        port=PG_PORT,
        sslmode="require",
    )
```

#### 5. Execute DDL and Verify

**Run schema.sql:**
```python
with get_connection() as conn:
    conn.autocommit = False
    with conn.cursor() as cur:
        for stmt in statements:
            cur.execute(stmt)
    conn.commit()  # Commit all DDL changes
```

**Verify persistence:**
```python
# Fresh connection to verify tables exist
with get_connection() as conn, conn.cursor() as cur:
    cur.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public';
    """)
    tables = [r[0] for r in cur.fetchall()]
    print(f"Tables: {tables}")
```

---

## Best Practices

### 1. Authentication Strategy

**For DDL operations (schema changes):**
```python
# ✅ ALWAYS use native password
password = dbutils.secrets.get("lakebase", "password")
conn = psycopg2.connect(
    host=PG_HOST,
    user="student",  # Database role, not email
    password=password,
    ...
)
```

**For read-only queries (exploration):**
```python
# ⚡ OAuth is acceptable
cred = w.postgres.generate_database_credential(endpoint=endpoint_name)
conn = psycopg2.connect(
    host=PG_HOST,
    user=w.current_user.me().user_name,  # Your email
    password=cred.token,
    ...
)
```

### 2. Secret Management

**Do:**
- ✅ Store all credentials in Databricks secrets
- ✅ Use descriptive scope names (`lakebase`, `production`, `dev`)
- ✅ Grant minimal necessary permissions
- ✅ Rotate passwords regularly
- ✅ Use different roles for dev/staging/production

**Don't:**
- ❌ Hardcode passwords in notebooks
- ❌ Commit credentials to Git
- ❌ Share passwords in plain text (Slack, email)
- ❌ Use the same password across environments
- ❌ Grant excessive permissions (ALL PRIVILEGES when SELECT is enough)

### 3. Connection Handling

**Use context managers:**
```python
# ✅ GOOD - automatic cleanup
with get_connection() as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM companies;")
        results = cur.fetchall()
# Connection automatically closed

# ❌ BAD - must manually close
conn = get_connection()
cur = conn.cursor()
cur.execute("SELECT * FROM companies;")
results = cur.fetchall()
cur.close()  # Easy to forget
conn.close()  # Easy to forget
```

**For DDL, use transactions:**
```python
with get_connection() as conn:
    conn.autocommit = False  # Start transaction
    try:
        with conn.cursor() as cur:
            cur.execute("CREATE TABLE companies (...);")
            cur.execute("CREATE TABLE filings (...);")
        conn.commit()  # Commit all changes together
    except Exception as e:
        conn.rollback()  # Rollback on error
        raise
```

### 4. Error Handling

**Check for common issues:**
```python
try:
    conn = get_connection()
except psycopg2.OperationalError as e:
    if "not a valid JWT encoding" in str(e):
        print("ERROR: Native auth not enabled or endpoint needs restart")
    elif "password authentication failed" in str(e):
        print("ERROR: Role doesn't exist or password is incorrect")
    elif "does not exist" in str(e):
        print("ERROR: Role not created yet")
    else:
        raise
```

### 5. Testing Strategy

**Always verify DDL persistence:**
```python
# Create tables
with get_connection() as conn:
    with conn.cursor() as cur:
        cur.execute("CREATE TABLE test_table (id INT);")
    conn.commit()

# Verify with FRESH connection
with get_connection() as conn:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_name = 'test_table';
        """)
        assert cur.fetchone() is not None, "Table not persisted!"
```

### 6. Documentation

**Document your setup:**
```markdown
## Lakebase Connection

**Authentication:** Native password (student role)
**Database:** databricks_postgres
**Secrets:** scope=lakebase, key=password

**Setup commands:**
1. Enable native auth on Lakebase project
2. Create role: `CREATE ROLE student WITH LOGIN PASSWORD '***';`
3. Store password: `databricks secrets put-secret lakebase password`
4. Grant permissions: `GRANT ALL ON DATABASE ... TO student;`
```

---

## Troubleshooting

### Problem: "Provided authentication token is not a valid JWT encoding"

**Cause:** Lakebase endpoint is rejecting the native password because:
1. Native auth not enabled on the project, OR
2. Endpoint hasn't restarted after enabling native auth

**Solution:**
```bash
# 1. Enable native auth
databricks lakebase update-project \
  --project-name stock-research-capstone \
  --enable-native-auth

# 2. Restart the endpoint
databricks lakebase stop-endpoint \
  --name projects/stock-research-capstone/branches/production/endpoints/primary
  
databricks lakebase start-endpoint \
  --name projects/stock-research-capstone/branches/production/endpoints/primary
```

### Problem: Tables created but immediately disappear

**Cause:** Using OAuth authentication with Lakebase Autoscaling
- Connection 1 → Instance A (creates table)
- Connection 2 → Instance B (table doesn't exist there yet)

**Solution:** Switch to native password authentication

### Problem: "role 'student' does not exist"

**Cause:** The Postgres role hasn't been created yet

**Solution:**
```sql
-- Connect with admin account first
CREATE ROLE student WITH LOGIN PASSWORD '<stored-in-secret: lakebase/password>';
GRANT ALL PRIVILEGES ON DATABASE databricks_postgres TO student;
```

### Problem: "password authentication failed for user 'student'"

**Cause:** Password mismatch between:
1. What was set when creating the role (`CREATE ROLE ... PASSWORD '...'`)
2. What's stored in Databricks secrets

**Solution:**
```bash
# Update the secret to match the role's password
databricks secrets put-secret lakebase password
# Paste the correct password
```

---

## Summary

### Key Takeaways

1. **Lakebase Autoscaling = Multiple Instances**
   - OAuth can route to different instances
   - DDL changes don't replicate immediately
   - Use native password for schema operations

2. **Authentication Methods**
   - **OAuth:** Quick queries, read-only, stateless
   - **Native Password:** DDL, transactions, production

3. **Secrets Management**
   - Store in Databricks secrets (workspace-level)
   - Never hardcode credentials
   - Use `dbutils.secrets.get(scope, key)`

4. **Postgres Roles**
   - Separate identity from workspace user
   - Persistent password (doesn't expire)
   - Fine-grained permissions (GRANT)

5. **Connection String**
   - Standard Postgres format
   - Retrieve password from secrets
   - Always use `sslmode=require`

### Quick Reference

**When to use Native Password Authentication:**
- ✅ Creating/modifying schema (DDL)
- ✅ Multi-statement transactions
- ✅ Long-lived application connections
- ✅ Data ingestion pipelines
- ✅ Production workloads

**When OAuth is acceptable:**
- ⚡ Ad-hoc SELECT queries
- ⚡ Exploratory data analysis
- ⚡ Notebook prototyping (no DDL)

---

## Additional Resources

- [Databricks Lakebase Documentation](https://docs.databricks.com/lakebase/)
- [PostgreSQL Authentication Methods](https://www.postgresql.org/docs/current/auth-methods.html)
- [Databricks Secrets API](https://docs.databricks.com/security/secrets/index.html)
- [psycopg2 Documentation](https://www.psycopg.org/docs/)

---

**Document Version:** 1.0  
**Last Updated:** 2026-08-08  
**Project:** AI Stock Market Research Assistant (Capstone)  
**Phase:** Phase 0 - Setup & Schema