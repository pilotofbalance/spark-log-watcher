import React, { useState, useEffect } from 'react';
import axios from 'axios';
import './App.css'; // Create App.css for styling

function App() {
  const [data, setData] = useState({ total: 0, page: 0, limit: 5, data: [] });
  const [currentPage, setCurrentPage] = useState(0);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    const fetchData = async () => {
      setLoading(true);
      setError(null);
      try {
        const response = await axios.post('http://localhost:8787/data', {
          page: currentPage,
          limit: data.limit,
        });
        setData(response.data);
      } catch (err) {
        setError(err);
      } finally {
        setLoading(false);
      }
    };

    fetchData();
  }, [currentPage, data.limit]);

  const handlePageChange = (newPage) => {
    setCurrentPage(newPage);
  };

  const handleLimitChange = (event) => {
    setData({ ...data, limit: parseInt(event.target.value, 10) });
    setCurrentPage(0); // Reset to first page on limit change
  };

  if (loading) return <p>Loading...</p>;
  if (error) return <p>Error: {error.message}</p>;

  return (
    <div className="container">
      <h1>Spark Events</h1>
      <div className="pagination-controls">
        <label htmlFor="limit">Items per page:</label>
        <select id="limit" value={data.limit} onChange={handleLimitChange}>
          <option value="5">5</option>
          <option value="10">10</option>
          <option value="20">20</option>
        </select>
        <button
          onClick={() => handlePageChange(currentPage - 1)}
          disabled={currentPage === 0}
        >
          Previous
        </button>
        <span>Page {currentPage + 1} of {Math.ceil(data.total / data.limit)}</span>
        <button
          onClick={() => handlePageChange(currentPage + 1)}
          disabled={currentPage >= Math.ceil(data.total / data.limit) - 1}
        >
          Next
        </button>
      </div>

      <table>
        <thead>
          <tr>
            <th>Event</th>
            <th>Timestamp</th>
            <th>Spark Version</th>
            <th>App Name</th>
            <th>App ID</th>
            <th>User</th>
            <th>Stage ID</th>
            <th>Job ID</th>
            <th>Executor ID</th>
            <th>Extra Data</th>
          </tr>
        </thead>
        <tbody>
          {data.data.map((item, index) => (
            <tr key={index}>
              <td>{item.Event}</td>
              <td>{new Date(item.Timestamp).toLocaleString()}</td>
              <td>{item.SparkVersion}</td>
              <td>{item.AppName}</td>
              <td>{item.AppId}</td>
              <td>{item.User}</td>
              <td>{item.StageID}</td>
              <td>{item.JobID}</td>
              <td>{item.ExecutorID}</td>
              <td>{item.ExtraData}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export default App;