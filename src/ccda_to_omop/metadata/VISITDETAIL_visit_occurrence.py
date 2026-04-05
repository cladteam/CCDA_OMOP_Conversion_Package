metadata = { }
# I am not sure if or why this is needed.
# We're not running visit detail at the moment and it is creating unneeded error messages.
#
not_today =  {
    "VISITDETAIL_visit_occurrence": {
        "root": {
            "expected_domain_id": "VisitDetail",
            "element": ''
        },
        'domain_id': {
            'config_type': 'CONSTANT',
            'constant_value' : 'Visit'
        }

    }
}
