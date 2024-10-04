from typing import Dict


class DisaggregatedAttachments:
    def __init__(self, save_test_data):
        if save_test_data:
            self._disaggregated_attachments = []
        else:
            self._disaggregated_attachments = None

    def add_disaggregated_attachment(
        self, disaggregated_attachment: Dict
    ) -> None:
        if self._disaggregated_attachments is None:
            return
        self._disaggregated_attachments.append(disaggregated_attachment)
