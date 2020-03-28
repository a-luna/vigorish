function makeChunkedList(inputList, chunkSize) {
  let chunkedList = []
  let totalChunks = (inputList.length / chunkSize) | 0
  let lastChunkIsUneven = inputList.length % chunkSize > 0
  if (lastChunkIsUneven) {
    totalChunks += 1
  }
  for (let i = 0; i < totalChunks; i++) {
    let start = i * chunkSize
    let end = start + chunkSize
    if (lastChunkIsUneven && i === totalChunks - 1) {
      end = inputList.length
    }
    chunkedList.push(inputList.slice(start, end))
  }
  return chunkedList
}

function makeIrregularChunkedList(inputList, minChunkSize, maxChunkSize) {
  let chunkedList = []
  let remainingItems = inputList.length
  let chunkSize = 0, start = 0, end = 0
  while (remainingItems > 0) {
    if (remainingItems > maxChunkSize) {
      chunkSize = getRandomInt(minChunkSize, maxChunkSize)
    }
    else {
      chunkSize = remainingItems
    }
    start = end
    end = end + chunkSize
    chunkedList.push(inputList.slice(start, end))
    remainingItems -= chunkSize
  }
  return chunkedList
}

  function getRandomInt(min, max) {
    min = Math.ceil(min);
    max = Math.floor(max);
    return Math.floor(Math.random() * (max - min)) + min;
  }

  module.exports = {
    makeChunkedList: makeChunkedList,
    makeIrregularChunkedList: makeIrregularChunkedList
  };
