import { getPluginPath } from './utils.js';

export async function testS3Connection() {
  const resultDiv = document.getElementById('s3TestResult');
  const button = document.querySelector('button[onclick="testS3Connection()"]');

  button.disabled = true;
  button.textContent = '🔄 Testing...';
  resultDiv.innerHTML = '<div class="loading">Testing S3 connection...</div>';

  try {
    const response = await fetch(`${getPluginPath()}/api/test-s3`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
    });

    const result = await response.json();

    if (result.success) {
      resultDiv.innerHTML = `
                <div class="success">
                    ✅ ${result.message}<br>
                    <strong>Bucket:</strong> ${result.bucket}<br>
                    <strong>Region:</strong> ${result.region}<br>
                    <strong>Key Prefix:</strong> ${result.keyPrefix}
                </div>
            `;
    } else {
      resultDiv.innerHTML = `<div class="error">❌ ${result.error}</div>`;
    }
  } catch (error) {
    resultDiv.innerHTML = `<div class="error">❌ Network error: ${error.message}</div>`;
  } finally {
    button.disabled = false;
    button.textContent = '🔗 Test S3 Connection';
  }
}
