import copy


class CaseloadJSON:
    def __init__(self, caseload: dict, save_test_data: bool = False) -> None:
        self._disaggregated_attachments = []
        if save_test_data:
            self._caseload = copy.copy(caseload)
            del self._caseload["measurements"]
            self._caseload["disaggregatedAttachments"] = self._disaggregated_attachments
        else:
            self._caseload = None
        self._save_test_data = save_test_data

    def add_disaggregated_attachment(self, disaggregated_attachment: dict) -> None:
        if self._save_test_data:
            self._disaggregated_attachments.append(disaggregated_attachment)
