const { read_latest_snapshot } = require('../../lib/snapshot_store');

module.exports = async function handler(req, res) {
  if (req.method && req.method !== 'GET') {
    res.setHeader('Allow', 'GET');
    return res.status(405).json({ detail: 'Method not allowed' });
  }

  try {
    const payload = await read_latest_snapshot();
    res.setHeader('Cache-Control', 'no-store');
    return res.status(200).json(payload);
  } catch (error) {
    return res.status(404).json({
      detail: error.message || 'No data available yet. Run the daily pipeline first.',
    });
  }
};
