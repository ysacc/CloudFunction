const functions = require('@google-cloud/functions-framework')
const { getAccessToken, loadFile } = require('./util.js')

functions.cloudEvent('main', async cloudEvent => {

    const file = cloudEvent.data
   // console.log(`Bucket: ${file.bucket}`)
    console.log(`File: ${file.name}`)
    const { access_token } = await getAccessToken()
   // console.log(`My access_token: ${access_token}`)
    //Load file in db
    const resultLoad = await loadFile(access_token, file.name)
    console.log(resultLoad)
    console.log('End')
})
