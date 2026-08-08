# Documentation Index

Documentation for the AI Stock Market Research Assistant capstone project.

## Phase 0 - Setup & Infrastructure

### Lakebase Authentication Guide
**File:** [`lakebase-authentication-guide.md`](./lakebase-authentication-guide.md)

**Essential reading for anyone working with Lakebase Postgres in this project.**

Covers:
- 🚨 **Critical lesson learned:** Why DDL operations require native password authentication
- Understanding Lakebase Autoscaling architecture (multi-instance routing)
- OAuth vs Native Password authentication - when to use which
- Databricks secrets management (where secrets are stored, how to use them)
- Postgres roles explained (authentication + authorization)
- Connection string anatomy
- Complete setup workflow
- Best practices and troubleshooting

**Quick reference:**

| Task | Use This Auth |
|------|---------------|
| CREATE/ALTER/DROP tables | ✅ Native Password |
| Data ingestion (INSERT) | ✅ Native Password |
| Multi-statement transactions | ✅ Native Password |
| Ad-hoc SELECT queries | ⚡ OAuth OK |
| Exploratory analysis | ⚡ OAuth OK |

---

## Project Structure

```
stock-research-capstone/
├── docs/
│   ├── README.md (this file)
│   └── lakebase-authentication-guide.md
├── notebooks/
│   └── 00_setup.ipynb (schema provisioning)
└── sql/
    └── schema.sql (Postgres DDL)
```

---

## Getting Started

1. **Read the authentication guide first** - it explains critical setup decisions
2. **Configure secrets** - store your Lakebase password in Databricks secrets
3. **Run 00_setup notebook** - provisions schema using native password auth
4. **Verify Phase 0 checkpoint** - all 8 tables should persist

---

## Questions?

If you encounter issues:

1. Check [`lakebase-authentication-guide.md`](./lakebase-authentication-guide.md) troubleshooting section
2. Verify native auth is enabled on Lakebase project
3. Confirm password is stored in secrets (scope: `lakebase`, key: `password`)
4. Check connection user is `student` role, not email

---

**Last Updated:** 2026-08-08  
**Phase:** Phase 0 - Setup & Schema