# Tools

Quick tools

### Convert icite db to sqlite

```bash
curl -fsL https://raw.githubusercontent.com/huanhe4096/tools/main/mk-icite-db.py | python - /path-to-metadata.csv /path-to-icite.db
```

### Dump pubdb

```bash
export MONGODB_URI=mongodb://user:pass@host:port && curl -fsL https://raw.githubusercontent.com/huanhe4096/tools/main/dump-pubdb.py | python - data.tsv
```