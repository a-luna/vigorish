<script>
  let rand = "No data";
  function getSeasonData() {
    fetch("./season/2019")
      .then(d => d.text())
      .then(d => (rand = d));
  }
</script>

<button on:click={getSeasonData}>Get data for 2019 MLB season</button>
<pre><code>{rand}</code></pre>
