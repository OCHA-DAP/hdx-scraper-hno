import copy
from typing import Dict


class CaseloadJSON:
    def __init__(self, caseload: Dict, save_test_data: bool = False) -> None:
        self.disaggregated_attachments = []
        if save_test_data:
            self.caseload = copy.copy(caseload)
            self.caseload["disaggregatedAttachments"] = (
                self.disaggregated_attachments
            )
        else:
            self.caseload = None
        self.save_test_data = save_test_data

    def add_disaggregated_attachment(
        self, disaggregated_attachment: Dict
    ) -> None:
        if self.save_test_data:
            self.disaggregated_attachments.append(disaggregated_attachment)
