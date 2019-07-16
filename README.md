# merge-sh-dbs

Tool to merge Sorting Hat databases


# Get database dumps

You need to have `cncf/json2hat-helm` cloned and its access secrets configured. You can find database details for both `development` and `staging` environments in AWS console.

Development environment:

- Shell into MariaDB pod: `AWS_PROFILE=lfproduct-dev KUBECONFIG=/root/.kube/config_lf kubectl run -it --rm --image=mariadb --restart=Never mariadb --env="SH_HOST=`cat ~/dev/go/src/github.com/cncf/json2hat-helm/json2hat-helm/secrets/SH_HOST.secret`" --env="SH_USER=`cat ~/dev/go/src/github.com/cncf/json2hat-helm/json2hat-helm/secrets/SH_USER.secret`" --env="SH_PASS=`cat ~/dev/go/src/github.com/cncf/json2hat-helm/json2hat-helm/secrets/SH_PASS.secret`" --env="SH_DB=`cat ~/dev/go/src/github.com/cncf/json2hat-helm/json2hat-helm/secrets/SH_DB.secret`" -- /bin/bash`.
- Dump database into file: `mysqldump --single-transaction -h$SH_HOST -u$SH_USER -p$SH_PASS $SH_DB > dump.sql`.
- Using another terminal copy dump from the K8s pod: `AWS_PROFILE=lfproduct-dev KUBECONFIG=/root/.kube/config_lf kubectl cp mariadb:dump.sql dump_dev.sql`.


Staging environment:

- Shell into MariaDB pod: `AWS_PROFILE=lfproduct-staging KUBECONFIG=/root/.kube/config_lf_stg kubectl run -it --rm --image=mariadb --restart=Never mariadb --env="SH_HOST=`cat ~/dev/go/src/github.com/cncf/json2hat-helm/json2hat-helm/secrets/SH_HOST.stg.secret`" --env="SH_USER=`cat ~/dev/go/src/github.com/cncf/json2hat-helm/json2hat-helm/secrets/SH_USER.secret`" --env="SH_PASS=`cat ~/dev/go/src/github.com/cncf/json2hat-helm/json2hat-helm/secrets/SH_PASS.secret`" --env="SH_DB=`cat ~/dev/go/src/github.com/cncf/json2hat-helm/json2hat-helm/secrets/SH_DB.secret`" -- /bin/bash`.
- Dump database into file: `mysqldump --single-transaction -h$SH_HOST -u$SH_USER -p$SH_PASS $SH_DB > dump.sql`.
- Using another terminal copy dump from the K8s pod: `AWS_PROFILE=lfproduct-staging KUBECONFIG=/root/.kube/config_lf_stg kubectl cp mariadb:dump.sql dump_staging.sql`.
