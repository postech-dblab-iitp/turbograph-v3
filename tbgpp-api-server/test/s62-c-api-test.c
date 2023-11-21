#include "s62.h"
#include <stdio.h>

int main() {
    s62_state state = s62_connect("/data/tpch/sf1/");
    printf("s62_connect() done\n");
    printf("state: %d\n", state);

    state = s62_is_connected();
    printf("s62_is_connected() done\n");
    printf("state: %d\n", state);

    s62_metadata* metadata;
    s62_num_metadata num_metadata = s62_get_metadata_from_catalog(NULL, false, false, &metadata);
    printf("s62_get_metadata_from_catalog() done\n");
    printf("num_metadata: %d\n", num_metadata);
    
    // Iterate over linked list, and print label name and type
    s62_metadata* current = metadata;
    while (current != NULL) {
        printf("label: %s, type: %d\n", current->label_name, current->type);
        current = current->next;
    }

    // For each label, get properties
    s62_metadata* current_metadata = metadata;
    for (int i = 0; i < num_metadata; i++) {
        printf("Getting properties for label: %s, type: %d\n", current_metadata->label_name, current_metadata->type);
        s62_property* properties;
        s62_num_properties num_properties = s62_get_property_from_catalog(current_metadata->label_name, current_metadata->type, &properties);
        printf("s62_get_properties_from_catalog() done\n");
        printf("num_properties: %d\n", num_properties);

        s62_property* current_property = properties;
        while (current_property != NULL) {
            printf("order: %d, property_name: %s, property_type: %d, property_sql_type: %s, precision: %d, scale: %d\n", 
                current_property->order, current_property->property_name, current_property->property_type, current_property->property_sql_type, current_property->precision, current_property->scale);
            current_property = current_property->next;
        }

        s62_close_property(properties);
        current_metadata = current_metadata->next;
    }

    s62_close_metadata(metadata);

    // Prepare query
    s62_prepared_statement* prep_stmt = s62_prepare("MATCH (item:LINEITEM) \
		WHERE item.L_SHIPDATE <= $date \
		RETURN \
			item.L_RETURNFLAG AS ret_flag, \
			item.L_LINESTATUS AS line_stat, \ 
			sum(item.L_QUANTITY) AS sum_qty, \
			sum(item.L_EXTENDEDPRICE) AS sum_base_price, \
			sum(item.L_EXTENDEDPRICE * (1 - item.L_DISCOUNT)) AS sum_disc_price, \
			sum(item.L_EXTENDEDPRICE*(1 - item.L_DISCOUNT)*(1 + item.L_TAX)) AS sum_charge, \
			avg(item.L_QUANTITY) AS avg_qty, \
			avg(item.L_EXTENDEDPRICE) AS avg_price, \
			avg(item.L_DISCOUNT) AS avg_disc, \
			COUNT(*) AS count_order \
		ORDER BY \
			ret_flag, \
			line_stat;");

    printf("s62_prepare_query() done\n");

    printf("Prepared statement original query: %s\n", prep_stmt->query);
    printf("Prepared statement plan: %s\n", prep_stmt->plan);
    printf("Prepared statement num_properties: %d\n", prep_stmt->num_properties);
    s62_property* current_property = prep_stmt->property;
    while (current_property != NULL) {
        if (current_property->label_name == NULL) {
            printf("order: %d, label_type: %d, property_name: %s, property_type: %d, property_sql_type: %s, precision: %d, scale: %d\n", 
                current_property->order, current_property->label_type, current_property->property_name, current_property->property_type, current_property->property_sql_type, current_property->precision, current_property->scale);
        }
        else {
            printf("order: %d, label_name: %s, label_type: %d, property_name: %s, property_type: %d, property_sql_type: %s, precision: %d, scale: %d\n", 
                current_property->order, current_property->label_name, current_property->label_type, current_property->property_name, current_property->property_type, current_property->property_sql_type, current_property->precision, current_property->scale);
        }
        current_property = current_property->next;
    }

    s62_close_prepared_statement(prep_stmt);

    // // Bind values
    // int lower_bound = 1;
    // int upper_bound = 100;
    // s62_bind_value(query, 1, lower_bound);
    // s62_bind_value(query, 2, upper_bound);
    // printf("s62_bind_value() done\n");

    // // Execute query
    // state = s62_execute(query);
    // printf("s62_execute_query() done\n");
    // printf("state: %d\n", state);

    // // Fetch results
    // printf("Fetching results...\n");
    // int value;
    // while (s62_fetch(query, &value)) {
    //     printf("Fetched row value: %d\n", value);
    // }
    // printf("s62_fetch() done\n");

    // // Disconnect
    // s62_disconnect();
    // printf("s62_disconnect() done\n");
    
    return 0;
}
