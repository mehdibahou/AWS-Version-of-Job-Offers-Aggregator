// pages/api/redshiftData.js
import { Client } from "pg";
import { StatusCodes } from "http-status-codes";

const redshift = new Client({
  user: "your-redshift-username",
  host: "your-redshift-host",
  database: "your-database",
  password: "your-redshift-password",
  port: 5439, // Redshift port number
});

redshift
  .connect()
  .then(() => {
    console.log("Connected to Redshift");
  })
  .catch((err) => {
    console.error("Error connecting to Redshift", err);
  });

const getJobs = async (req, res) => {
  try {
    const query = "SELECT * FROM your_redshift_table"; // Replace with your Redshift query
    const result = await redshift.query(query);

    // Process the Redshift query result as needed
    const data = result.rows;

    res.status(StatusCodes.OK).json({ data });
  } catch (error) {
    console.error("Redshift query error:", error);
    res
      .status(StatusCodes.INTERNAL_SERVER_ERROR)
      .json({ error: "Internal Server Error" });
  }
};

module.exports = {
  getJobs,
};
