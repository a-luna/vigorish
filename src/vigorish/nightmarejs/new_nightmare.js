const Nightmare = require('nightmare');

Nightmare.action('waitUntilNetworkIdle',
  function (_name, _options, parent, win, _renderer, done) {
    parent.respondTo('waitUntilNetworkIdle', (waitTime, done) => {
      let lastRequestTime = Date.now();
      win.webContents.on('did-get-response-details', () => {
        lastRequestTime = Date.now();
      });

      const check = () => {
        const now = Date.now();
        const elapsedTime = now - lastRequestTime;
        if (elapsedTime >= waitTime) {
          done();
        } else {
          setTimeout(check, waitTime - elapsedTime);
        }
      }
      setTimeout(check, waitTime);
    });

    done();
  },
  function (waitTime, done) {
    if (!done) {
      done = waitTime;
      waitTime = 500;
    }
    this.child.call('waitUntilNetworkIdle', waitTime, done);
  });

module.exports = Nightmare;
