"""GraphQL query documents for the Humanitarian Action Fabric GraphQL API.

See docs/decisions/0001-migrate-to-fabric-graphql-api.md for why this replaced
the old HPC REST API, and 0003/0004 for why the fields selected here
(HasDisaggregatedData, CustomReference) are used the way they are.
"""

PLAN_DISCOVERY_QUERY = """
query PlanDiscovery($year: Int!, $first: Int!, $after: String) {
  plans(
    filter: {
      period: { CalendarYear: { eq: $year } }
      PlanType: {
        in: ["Humanitarian response plan", "Humanitarian needs and response plan"]
      }
    }
    first: $first
    after: $after
  ) {
    items {
      Id
      PlanType
      ReleasedDate
      location(first: 10, filter: { AdminLevel: { eq: 0 } }) {
        items {
          ISO3
        }
      }
    }
    hasNextPage
    endCursor
  }
}
"""

CASELOAD_ATTACHMENTS_QUERY = """
query CaseloadAttachments($planId: Int!, $first: Int!) {
  attachments(
    filter: { PlanId: { eq: $planId }, AttachmentType: { eq: "Caseload" } }
    first: $first
  ) {
    items {
      Id
      Name
      EntityMainType
      CustomReference
      HasDisaggregatedData
    }
  }
}
"""

CASELOAD_FACTS_QUERY = """
query CaseloadFacts($planId: Int!, $first: Int!, $after: String) {
  attachmentFacts(
    filter: {
      attachment: { PlanId: { eq: $planId }, AttachmentType: { eq: "Caseload" } }
    }
    first: $first
    after: $after
  ) {
    items {
      AttachmentId
      IsTotal
      ValueNum
      LocationId
      location {
        Name
        AdminLevel
        Pcode
      }
      GenderId
      gender {
        Name
      }
      AgeGroupId
      ageGroup {
        Name
      }
      PopulationStatusId
      populationStatus {
        Name
      }
      SettlementTypeId
      settlementType {
        Name
      }
      DisabilityStatusId
      disabilityStatus {
        Name
      }
      MaternalStatusId
      maternalStatus {
        Name
      }
      MetricTypeId
      metricType {
        Name
        HPCType
      }
    }
    hasNextPage
    endCursor
  }
}
"""
