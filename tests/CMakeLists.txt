## TESTS ##
# What every test in this directory links against.
set(SC_TEST_LINK_LIBRARIES sc-kafka)

# Tests are local-only: they do not require a Kafka broker.
add_sc_test(kafka LABELS unit TIMEOUT 120)
