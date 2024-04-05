from typing import Dict


class DisaggregatedAttachments:
    def __init__(self, save_test_data):
        if save_test_data:
            self.disaggregated_attachments = []
        else:
            self.disaggregated_attachments = None

    def add_disaggregated_attachment(
        self, disaggregated_attachment: Dict
    ) -> None:
        if self.disaggregated_attachments is None:
            return
        self.disaggregated_attachments.append(disaggregated_attachment)
