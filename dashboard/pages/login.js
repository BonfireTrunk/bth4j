import { api, setSession } from '../api.js';

const { ref } = Vue;

export default {
  setup() {
    const user = ref('');
    const password = ref('');
    const busy = ref(false);
    const error = ref('');

    async function submit() {
      busy.value = true;
      error.value = '';
      try {
        const res = await api.login(user.value, password.value);
        setSession(res);
        password.value = '';
        location.hash = '#/overview';
      } catch (e) {
        error.value = e.message;
      } finally {
        busy.value = false;
      }
    }
    return { user, password, busy, error, submit };
  },
  template: `
  <div class="login">
    <form class="card" @submit.prevent="submit">
      <h1>bth4j dashboard</h1>
      <label>User <input v-model="user" type="text" autocomplete="username" autofocus required></label>
      <label>Password <input v-model="password" type="password" autocomplete="current-password" required></label>
      <p v-if="error" class="err" role="alert">{{ error }}</p>
      <button type="submit" class="primary" :disabled="busy">{{ busy ? 'Signing in...' : 'Sign in' }}</button>
    </form>
  </div>`,
};
