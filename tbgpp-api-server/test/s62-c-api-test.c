#include "s62.h"
#include <stdio.h>

int main() {
    s62_state state = s62_connect("/data/tpch/sf1/");
    printf("s62_connect() done\n");
    printf("state: %d\n", state);

    state = s62_is_connected();
    printf("s62_is_connected() done\n");
    printf("state: %d\n", state);

    // Prepare query
    s62_prepared_statement* query = s62_prepare("SELECT value FROM numbers WHERE value BETWEEN ? AND ?;");
    printf("s62_prepare_query() done\n");

    // Bind values
    int lower_bound = 1;
    int upper_bound = 100;
    s62_bind_value(query, 1, lower_bound);
    s62_bind_value(query, 2, upper_bound);
    printf("s62_bind_value() done\n");

    // Execute query
    state = s62_execute(query);
    printf("s62_execute_query() done\n");
    printf("state: %d\n", state);

    // Fetch results
    printf("Fetching results...\n");
    int value;
    while (s62_fetch(query, &value)) {
        // Assuming s62_fetch() populates 'value' with the integer from the current row
        printf("Fetched row value: %d\n", value);
    }
    printf("s62_fetch() done\n");

    // Disconnect
    s62_disconnect();
    printf("s62_disconnect() done\n");
    
    return 0;
}
