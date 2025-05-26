import copy
from typing import Dict


class CaseloadJSON:
    def __init__(self, caseload: Dict, save_test_data: bool = False) -> None:
        self._disaggregated_attachments = []
        if save_test_data:
            self._caseload = copy.copy(caseload)
            self._caseload["disaggregatedAttachments"] = self._disaggregated_attachments
        else:
            self._caseload = None
        self._save_test_data = save_test_data

    def add_disaggregated_attachment(self, disaggregated_attachment: Dict) -> None:
        if self._save_test_data:
            self._disaggregated_attachments.append(disaggregated_attachment)
