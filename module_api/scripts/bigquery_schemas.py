# -*- coding: utf-8 -*-
#
# Run this script by running this in a terminal:
# python -m scripts.bigquery_schemas

import scripts


class BigQuerySchemas(scripts.ScriptBase):
    def main(self):
        from roger import config
        from roger_common import bigquery_api, events

        credentials = self.get_oauth2_credentials()

        bq = bigquery_api.BigQueryClient(
            project_id=config.BIGQUERY_PROJECT,
            dataset_id=config.BIGQUERY_DATASET,
            credentials=credentials,
            )

        all_events = [
            events.AccountActivatedV1,
            events.AccountActivatedV2,
            events.ChallengeV1,
            events.ContentV1,
            events.ContentV2,
            events.ContentActivityV1,
            events.ContentFirstV1,
            events.ContentRequestV1,
            events.ContentVoteV1,
            events.DeviceEventV1,
            events.FikaLoginV1,
            events.FikaLoginV2,
            events.FikaLoginV3,
            events.InviteV1,
            events.NewAccountV1,
            events.NotificationV1,
            events.OperatorV1,
            events.StreamV2,
            events.StreamV3,
            events.StreamTokenV1,
            events.TokenExchangeV1,
            events.NewAccountV1,
            events.UserActionV1,
            events.WalletPaymentV1,
        ]

        for event in all_events:
            try:
                bq.create_table(event, partitioning_type='DAY')
                print '- Created table for %s' % (event,)
            except Exception as e:
                print '- Failed to create table for %s: %s' % (event, e)


if __name__ == '__main__':
    BigQuerySchemas().run()
