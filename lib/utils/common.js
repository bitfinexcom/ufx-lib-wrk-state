function isObjetEmpty (live) {
  for(let _lk in live) {
    if(obj.hasOwnProperty(_lk)) {
      return false
    }
  }

  return true
}

module.exports = {
  isObjetEmpty: isObjetEmpty
}
