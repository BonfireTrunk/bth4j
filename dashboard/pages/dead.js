import { makeTasksPage } from './tasks.js';

export default makeTasksPage({ fixedState: 'DEAD', title: 'Dead tasks', basePath: 'dead', bulkRetry: true });
