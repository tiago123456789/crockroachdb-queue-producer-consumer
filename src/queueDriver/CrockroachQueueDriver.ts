import { Message, QueueDriver } from "../type";

class CrockroachQueueDriver implements QueueDriver {
  constructor(private connection: any, private schema: string = "public") {}

  /**
   * Send the message
   * @param queueName The name of the queue
   * @param message The message
   * @returns Promise<{ error: any }>
   */
  async send(
    queueName: string,
    message: { [key: string]: any }
  ): Promise<{ error: any }> {
    try {
      await this.connection(queueName).insert({ payload: message });

      return { error: null };
    } catch (error) {
      console.log(error);
      return { error };
    }
  }

  /**
   * Send the messages in batch
   * @param queueName The name of the queue
   * @param messages The message
   * @returns Promise<{ error: any }>
   */
  async sendBatch(
    queueName: string,
    messages: Array<{ [key: string]: any }>
  ): Promise<{ error: any }> {
    try {
      const items = messages.map((message) => ({ payload: message }));
      await this.connection(queueName).insert(items);

      return { error: null };
    } catch (error) {
      console.log(error);
      return { error };
    }
  }

  /**
   * Get the message
   * @param queueName The name of the queue
   * @param visibilityTime The visibility time of the message
   * @param totalMessages The total messages to get
   * @returns Promise<{ data: Message[], error: any }>
   */
  async get(
    queueName: string,
    visibilityTime: number,
    totalMessages: number
  ): Promise<{ data: Message[]; error: any }> {
    try {
      console.log(visibilityTime, totalMessages);
      const register = await this.connection.raw(
        `
            WITH next_job AS (
            SELECT id
            FROM ?? as jobs
            WHERE
                (
                    status = 'pending'
                    OR (status = 'in_progress' AND visible_at <= now())
                )
            ORDER BY created_at
            LIMIT ?
            FOR UPDATE SKIP LOCKED
        )
        UPDATE jobs
        SET status = 'in_progress',
            updated_at = now(),
            visible_at = now() + ?,
            retry_count = retry_count + 1
        FROM next_job
        WHERE jobs.id = next_job.id   
        RETURNING jobs.*;
    `,
        [
          queueName,
          totalMessages,
          this.connection.raw(`interval '${visibilityTime} seconds'`),
        ]
      );

      if (!register.rows) {
        return { data: [], error: null };
      }

      const items: Message[] = [];
      for (const row of register.rows) {
        items.push({
          msg_id: row.id,
          read_ct: row.retry_count,
          enqueued_at: row.created_at,
          vt: row.visible_at,
          message: row.payload, // Assuming the message content is stored in a column named 'payload' or '
        });
      }

      return { data: items, error: null };
    } catch (error) {
      return { data: [], error };
    }
  }

  /**
   * Pop the message
   * @param queueName The name of the queue
   * @returns Promise<{ data: Message[], error: any }>
   */
  async pop(queueName: string): Promise<{ data: Message[]; error: any }> {
    const items = await this.get(queueName, 60, 1);
    if (items.data.length > 0) {
      await this.delete(queueName, items.data[0].msg_id);
    }

    return items;
  }

  /**
   * Delete the message
   * @param queueName The name of the queue
   * @param messageID The message ID
   * @returns Promise<{ error: any }>
   */
  async delete(queueName: string, messageID: number): Promise<{ error: any }> {
    try {
      await this.connection(queueName).where("id", messageID).del();

      return { error: null };
    } catch (error) {
      console.log(error);
      return { error };
    }
  }
}

export default CrockroachQueueDriver;
