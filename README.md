# merge-sh-dbs

Tool to merge Sorting Hat databases


# Get database dumps

You need to have `cncf/json2hat-helm` cloned and its access secrets configured. You can find database details for both `development` and `staging` environments in AWS console.

Development environment:

- Shell into MariaDB pod: `AWS_PROFILE=lfproduct-dev KUBECONFIG=/root/.kube/config_lf kubectl run -it --rm --image=mariadb --restart=Never mariadb --env="SH_HOST=`cat ~/dev/go/src/github.com/cncf/json2hat-helm/json2hat-helm/secrets/SH_HOST.secret`" --env="SH_USER=`cat ~/dev/go/src/github.com/cncf/json2hat-helm/json2hat-helm/secrets/SH_USER.secret`" --env="SH_PASS=`cat ~/dev/go/src/github.com/cncf/json2hat-helm/json2hat-helm/secrets/SH_PASS.secret`" --env="SH_DB=`cat ~/dev/go/src/github.com/cncf/json2hat-helm/json2hat-helm/secrets/SH_DB.secret`" -- /bin/bash`.
- Dump database into file: `mysqldump --single-transaction -h$SH_HOST -u$SH_USER -p$SH_PASS $SH_DB > dump.sql`.
- Dump database structure into file: `mysqldump -d -h$SH_HOST -u$SH_USER -p$SH_PASS $SH_DB > struct.sql`.
- Using another terminal copy dump from the K8s pod: `AWS_PROFILE=lfproduct-dev KUBECONFIG=/root/.kube/config_lf kubectl cp mariadb:dump.sql dump_dev.sql`.
- Using another terminal copy structure dump from the K8s pod: `AWS_PROFILE=lfproduct-dev KUBECONFIG=/root/.kube/config_lf kubectl cp mariadb:struct.sql dump_struct.sql`.


Staging environment:

- Shell into MariaDB pod: `AWS_PROFILE=lfproduct-staging KUBECONFIG=/root/.kube/config_lf_stg kubectl run -it --rm --image=mariadb --restart=Never mariadb --env="SH_HOST=`cat ~/dev/go/src/github.com/cncf/json2hat-helm/json2hat-helm/secrets/SH_HOST.stg.secret`" --env="SH_USER=`cat ~/dev/go/src/github.com/cncf/json2hat-helm/json2hat-helm/secrets/SH_USER.secret`" --env="SH_PASS=`cat ~/dev/go/src/github.com/cncf/json2hat-helm/json2hat-helm/secrets/SH_PASS.secret`" --env="SH_DB=`cat ~/dev/go/src/github.com/cncf/json2hat-helm/json2hat-helm/secrets/SH_DB.secret`" -- /bin/bash`.
- Dump database into file: `mysqldump --single-transaction -h$SH_HOST -u$SH_USER -p$SH_PASS $SH_DB > dump.sql`.
- Using another terminal copy dump from the K8s pod: `AWS_PROFILE=lfproduct-staging KUBECONFIG=/root/.kube/config_lf_stg kubectl cp mariadb:dump.sql dump_staging.sql`.


# Restore dumps

Restore dev and staging dumps, for example locally:

- `mysql`: `create database dev`, `create database staging`, `mysql dev < dump_dev.sql`, `mysql staging < dump_staging.sql`, `create database merged`, `mysql merged < dump_struct.sql`.


# Merge databases

There are 3 prefixes: `SH1_`, `SH2_` and `SH_`. `SH1_` is used for the first input DB, `SH2_` is used for the second input DB, `SH_` is used for the output DB. program will merge two input databases into output database.

Database with `SH1_` has a higher priority than `SH2_` when resolving conflicts (but only when we cannot solve conflict using newer record).

Setting Sorting Hat database parameters: you can either provide full database connect string/dsn via `SH_DSN=...` or provide all or some paramaters individually, via `SH_*` environment variables. `SH_DSN=..` has a higher priority and no `SH_*` parameters are used if `SH_DSN` is provided. When using `SH_*` parameters, only `SH_PASS` is required, all other parameters have default values.

Sorting Hat database connection parameters (example with prefix `SH_`, you can replace with `SH1_` or `SH2_`):

- `SH_DSN` - provides full database connect string, for example: `SH_DSN='shuser:shpassword@tcp(shhost:shport)/shdb?charset=utf8'`
- `SH_USER` - user name, defaults to `shuser`.
- `SH_PASS` - password - required.
- `SH_PROTO` - protocol, defaults to `tcp`.
- `SH_HOST` - host, defaults to `localhost`.
- `SH_PORT` - port, defaults to `3306`.
- `SH_DB` - database name, defaults to `shdb`.
- `SH_PARAMS` - additional parameters that can be specified via `?param1=value1&param2=value2&...&paramN=valueN`, defaults to `?charset=utf8`. You can use `SH_PARAMS='-'` to specify empty params.


# Running merge

Many possible connect strings:

- Using TCP: `SH1_USER=root SH2_USER=root SH_USER=root SH1_PASS=... SH2_PASS=... SH_PASS=... SH1_DB=dev SH2_DB=staging SH_DB=merged ./merge-sh-dbs`.
- Using unix sockets without passwords (fastest local option): `SH1_DSN='root@unix(/var/run/mysqld/mysqld.sock)/dev' SH2_DSN='root@unix(/var/run/mysqld/mysqld.sock)/staging' SH_DSN='root@unix(/var/run/mysqld/mysqld.sock)/merged' ./merge-sh-dbs`.
