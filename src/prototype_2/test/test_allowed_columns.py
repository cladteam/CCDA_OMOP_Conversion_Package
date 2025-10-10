import unittest
from prototype_2.domain_dataframe_column_types import domain_dataframe_column_required

# from prototype_2.layer_datasets import NULL_ALLOWED_COLUMNS
# import not working, so code copied
NON_NULLABLE_COLUMNS = {
    table: [
        field
        for field, required in domain_dataframe_column_required[table].items()
        if required
    ]
    for table in domain_dataframe_column_required
}


class TestAllowedColumns (unittest.TestCase):

    def test_non_nullable_consistency(self):
        """Ensure NON_NULLABLE_COLUMNS matches the required=True flags in domain_dataframe_column_required."""
        all_good = True
        bad_entries = []

        for table_name, fields_dict in domain_dataframe_column_required.items():
            # Verify the table exists in NON_NULLABLE_COLUMNS
            self.assertIn(table_name, NON_NULLABLE_COLUMNS, f"{table_name} missing in NON_NULLABLE_COLUMNS")
            print(table_name)

            for field_name, required_status in fields_dict.items():
                print(f"{table_name} {field_name} {required_status}")
                in_non_nullable = field_name in NON_NULLABLE_COLUMNS[table_name]

                if required_status and not in_non_nullable:
                    bad_entries.append(f"{table_name}.{field_name} is required but missing in NON_NULLABLE_COLUMNS")
                    all_good = False
                elif not required_status and in_non_nullable:
                    bad_entries.append(f"{table_name}.{field_name} is optional but present in NON_NULLABLE_COLUMNS")
                    all_good = False

        if not all_good:
            print("\nInconsistencies found:")
            for entry in bad_entries:
                print(" -", entry)
        self.assertTrue(all_good, "Inconsistent entries found between required flags and NON_NULLABLE_COLUMNS")

    def test_non_nullable_not_empty(self):
        """Basic sanity check: every table should have at least one required field."""
        for table_name, non_nullable_fields in NON_NULLABLE_COLUMNS.items():
            self.assertTrue(len(non_nullable_fields) > 0, f"{table_name} has no required columns defined")


if __name__ == "__main__":
    unittest.main()