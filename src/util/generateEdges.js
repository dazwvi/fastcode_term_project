const maxIndex = 6012
const edgesCount = 500000

function getRandomInt(max) {
  return Math.floor(Math.random() * Math.floor(max)) + 1;
}

for (let i = 0; i < edgesCount; i++) {
  const from = getRandomInt(maxIndex) 
  const to = getRandomInt(maxIndex) 
  console.log('' + from + ' ' + to)
}
