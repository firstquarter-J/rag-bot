import unittest

import boxer.company.prompt_security as legacy_prompt_security
import boxer.company.utils as legacy_utils
import boxer.routers.company.barcode_log as legacy_barcode_log
import boxer_company.prompt_security as company_prompt_security
import boxer_company.utils as company_utils
import boxer_company.routers.barcode_log as company_barcode_log


class CompanyModuleCompatTests(unittest.TestCase):
    def test_legacy_company_module_reexports_new_prompt_security(self) -> None:
        self.assertIs(
            legacy_prompt_security.looks_like_prompt_exfiltration_question,
            company_prompt_security.looks_like_prompt_exfiltration_question,
        )

    def test_legacy_company_utils_reexports_private_helpers(self) -> None:
        self.assertTrue(hasattr(legacy_utils, "_extract_barcode"))
        self.assertIs(legacy_utils._extract_barcode, company_utils._extract_barcode)

    def test_legacy_company_router_reexports_new_router_functions(self) -> None:
        self.assertTrue(hasattr(legacy_barcode_log, "_extract_log_date"))
        self.assertIs(legacy_barcode_log._extract_log_date, company_barcode_log._extract_log_date)


if __name__ == "__main__":
    unittest.main()
