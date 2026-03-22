document.addEventListener('DOMContentLoaded', function() {
    document.getElementById('loginForm').addEventListener('submit', async function(e) {
        e.preventDefault();
        
        const email = document.getElementById('email').value;
        const password = document.getElementById('password').value;
        const errorMessage = document.getElementById('error-message');
        const loginButton = document.getElementById('loginButton');
        
        // Disable submit button and show loading state
        loginButton.disabled = true;
        loginButton.textContent = 'Logging in...';
        errorMessage.style.display = 'none';

        try {
            const response = await fetch('/login', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({ email, password })
            });

            const data = await response.json();

            if (response.ok && data.success) {
                // Successful login - redirect to home page
                window.location.href = data.redirect || '/';
            } else {
                // Show error message
                errorMessage.textContent = data.message || 'Login failed. Please try again.';
                errorMessage.style.display = 'block';
            }
        } catch (error) {
            errorMessage.textContent = 'An error occurred. Please try again.';
            errorMessage.style.display = 'block';
        } finally {
            // Re-enable submit button
            loginButton.disabled = false;
            loginButton.textContent = 'Login';
        }
    });
}); 