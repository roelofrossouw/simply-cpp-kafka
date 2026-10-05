## TESTS ##
# What every test in this directory links against.
set(SC_TEST_LINK_LIBRARIES sc-kafka)

# Tests labelled "database" need a reachable PostgreSQL server; run only the rest with
#     ctest -LE database
add_sc_test(kafka TIMEOUT 120)
