kubectl run polaris-bootstrap \
  -n ${POLARIS_NAMESPACE} \
  --image=apache/polaris-admin-tool:latest \
  --restart=Never \
  --rm -it \
  --env="polaris.persistence.type=relational-jdbc" \
  --env="quarkus.datasource.username=$(kubectl get secret polaris-persistence \
    -n ${POLARIS_NAMESPACE} -o jsonpath='{.data.username}' | base64 -d)" \
  --env="quarkus.datasource.password=$(kubectl get secret polaris-persistence \
    -n ${POLARIS_NAMESPACE} -o jsonpath='{.data.password}' | base64 -d)" \
  --env="quarkus.datasource.jdbc.url=$(kubectl get secret polaris-persistence \
    -n ${POLARIS_NAMESPACE} -o jsonpath='{.data.jdbcUrl}' | base64 -d)" \
  -- \
  bootstrap -r ${POLARIS_REALM} -c ${POLARIS_REALM},root,s3cr3t