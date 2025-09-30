import unittest
import datetime
import prototype_2.data_driven_parse as DD

class TestVisitReconciliation(unittest.TestCase):

    def setUp(self):
        # Common visit dicts
        self.single_visit = [{
            "visit_occurrence_id": 1,
            "visit_start_date": datetime.date(2025, 9, 1),
            "visit_start_datetime": datetime.datetime(2025, 9, 1, 8, 0, 0),
            "visit_end_date": datetime.date(2025, 9, 1),
            "visit_end_datetime": datetime.datetime(2025, 9, 1, 17, 0, 0),
        }]

        self.two_overlapping_visits = [
            {
                "visit_occurrence_id": 1,
                "visit_start_date": datetime.date(2025, 9, 1),
                "visit_start_datetime": datetime.datetime(2025, 9, 1, 8, 0, 0),
                "visit_end_date": datetime.date(2025, 9, 1),
                "visit_end_datetime": datetime.datetime(2025, 9, 1, 17, 0, 0),
            },
            {
                "visit_occurrence_id": 2,
                "visit_start_date": datetime.date(2025, 9, 1),
                "visit_start_datetime": datetime.datetime(2025, 9, 1, 9, 0, 0),
                "visit_end_date": datetime.date(2025, 9, 1),
                "visit_end_datetime": datetime.datetime(2025, 9, 1, 18, 0, 0),
            },
        ]

    def test_single_match_assigns_fk(self):
        domain_dict = [{
            "measurement_id": 100,
            "measurement_date": datetime.date(2025, 9, 1),
            "measurement_datetime": datetime.datetime(2025, 9, 1, 10, 0, 0),
        }]

        DD.reconcile_visit_FK_with_specific_domain("Measurement", domain_dict, self.single_visit)
        self.assertEqual(domain_dict[0]["visit_occurrence_id"], 1)

    def test_no_match_leaves_fk_null(self):
        domain_dict = [{
            "measurement_id": 101,
            "measurement_date": datetime.date(2025, 9, 2),  # Outside visit window
            "measurement_datetime": datetime.datetime(2025, 9, 2, 10, 0, 0),
        }]

        DD.reconcile_visit_FK_with_specific_domain("Measurement", domain_dict, self.single_visit)
        self.assertNotIn("visit_occurrence_id", domain_dict[0])

    def test_multiple_matches_leaves_fk_null(self):
        domain_dict = [{
            "measurement_id": 102,
            "measurement_date": datetime.date(2025, 9, 1),
            "measurement_datetime": datetime.datetime(2025, 9, 1, 10, 30, 0),
        }]

        DD.reconcile_visit_FK_with_specific_domain("Measurement", domain_dict, self.two_overlapping_visits)
        self.assertNotIn("visit_occurrence_id", domain_dict[0])

    def test_unk_end_date_event(self):
        """Missing end date should fall back to start date and still match."""
        domain_dict = [{
            "drug_exposure_id": 200,
            "drug_exposure_start_date": datetime.date(2025, 9, 1),
            "drug_exposure_start_datetime": datetime.datetime(2025, 9, 1, 9, 0, 0),
            "drug_exposure_end_date": None,
            "drug_exposure_end_datetime": None,
        }]

        DD.reconcile_visit_FK_with_specific_domain("Drug", domain_dict, self.single_visit)
        self.assertEqual(domain_dict[0]["visit_occurrence_id"], 1)

if __name__ == "__main__":
    unittest.main()