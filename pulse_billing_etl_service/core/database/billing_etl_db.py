from datetime import datetime

from core.database.database_class import DB
from core.utility import logger
from sqlalchemy.sql import text


class BillingETLDB:
    def __init__(self, db: DB):
        self.db = db

    def save(
        self,
        status: str,
        org_id: int,
        project_id: str,
        end_date_time: datetime = None,
    ):
        """Saves the status of the ETL job in the billing_etl_status table."""

        values = {
            "org_id": org_id,
            "project_id": project_id,
            "status": status,
            "end_date_time": end_date_time
        }

        # Remove keys with None values to avoid inserting NULLs unless intended
        filtered_values = {k: v for k, v in values.items() if v is not None}

        cols_arr = [f":{key}" for key in filtered_values.keys()]
        query = text(
            f'''INSERT INTO billing_etl_status ({', '.join(filtered_values.keys())})
                VALUES ({', '.join(cols_arr)})'''
        )

        res, err = self.db.execute(query, filtered_values)

        if err is not None:
            logger.info_log(f"Error saving ETL status: {err}")
            return err

        return res

    def get_last_success_timestamp(self, org_id: int, project_id: str):
        query = text("""
            SELECT end_date_time FROM billing_etl_status
            WHERE org_id = :org_id AND project_id = :project_id AND status = 'SUCCESS'
            ORDER BY end_date_time DESC
            LIMIT 1
        """)
        res, err = self.db.execute(
            query, {"org_id": org_id, "project_id": project_id})
        if err is not None or not res:
            return None

        row = res.first()
        if row:
            # Access with ._mapping for dict-like access
            return row._mapping['end_date_time']
        return None

    def get_client_billing_alert_config(self, org_id: int):
        query = text(
            """
            SELECT 
                projectid,
                billingdataset,
                tableid,
                pulsebillingdataset,
                customerserviceaccountid
            FROM billing_alerts_setting
            WHERE org_id = :org_id
            """
        )

        res, err = self.db.execute(query, {"org_id": org_id})

        if err is not None:
            return None, err

        row = res.fetchone()
        if row is None:
            return None, "Client billing alert config not found"

        # Convert Row to dict
        result = dict(row._mapping)
        print(f"DEBUG get_client_billing_alert_config dict result: {result}")
        return result, None

    def update_step_completed_in_user_stepper_form_step_status(self, stepid: int, org_id: int, step_completed: bool):
        logger.info_log(
            "Updating step status in user_stepper_form_step_status...")

        query = text("""
            UPDATE user_stepper_form_step_status
            SET step_completed = :step_completed
            WHERE stepid = :stepid AND org_id = :org_id
        """)

        params = {
            "step_completed": step_completed,
            "stepid": stepid,
            "org_id": org_id
        }

        try:
            res, err = self.db.execute(query, params)

            if err is not None:
                logger.error_log(f"Failed to update step status: {err}")
                return None, err

            logger.info_log(
                f"Update successful. Rows affected: {res.rowcount}")
            return {"rows_affected": res.rowcount}, None

        except Exception as e:
            logger.error_log(f"Exception during step status update: {str(e)}")
            return None, str(e)

    def update_table_and_dataset_values_in_db(self, org_id: int, project_id: str, updated_values: dict):
        logger.info_log(
            "Attempting to update billing_alerts_setting dataset values...")

        valid_columns = {"pulsebillingdataset", "pulsetableid"}
        update_fields = {
            k: v for k, v in updated_values.items() if k in valid_columns
        }

        if not update_fields:
            logger.info_log("No valid fields to update.")
            return None, "No valid fields to update"

        # Dynamically build SET clause
        set_clause = ", ".join(
            [f"{key} = :{key}" for key in update_fields.keys()])

        query = text(f"""
            UPDATE billing_alerts_setting
            SET {set_clause}
            WHERE org_id = :org_id AND projectid = :project_id
        """)

        params = update_fields.copy()
        params.update({"org_id": org_id, "project_id": project_id})

        try:
            res, err = self.db.execute(query, params)

            if err is not None:
                logger.error_log(f"Failed to update dataset and tableid values: {err}")
                return None, err

            logger.info_log(
                f"Update successful. Rows affected: {res.rowcount}")
            return {"rows_affected": res.rowcount}, None

        except Exception as e:
            logger.error_log(
                f"Exception occurred while updating dataset values: {str(e)}")
            return None, str(e)
