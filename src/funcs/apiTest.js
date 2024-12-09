const axios = require("axios");
require("dotenv").config();

const apikey = process.env.APIKEY;
const baseUrl = process.env.BASEURL;
const scriptsBase = process.env.NAMEBASE;

// Full address
const url = `${baseUrl}${scriptsBase}?apikey=${apikey}`;

async function main() {
    try {
        // Fetch response
        const response = await axios.get(url);
        const rawResponse = response.data;

        // Ensure rawResponse is a string
        const responseString = typeof rawResponse === "string" ? rawResponse : JSON.stringify(rawResponse);

        // Split lines by ',' and store in an array
        const lines = responseString.split(',');

        const scriptUrls = [];

        // Extract script URLs from rdf:resource lines
        lines.forEach((line) => {
            if (line.includes('rdf:resource')) {
                let scriptUrl = line.match(/http:.*"/)?.[0]?.slice(0, -1); // Get URL and remove trailing "
                scriptUrl = scriptUrl.replace(/\\/g, ''); // Remove escaped slashes
                if (scriptUrl) scriptUrls.push(scriptUrl);
            }
        });

        const scripts = {};
        let counter = 0;

        // Fetch each script
        for (const scriptUrl of scriptUrls) {
            const fullUrl = `${scriptUrl}?apikey=${apikey}`;
            const scriptResponse = await axios.get(fullUrl);
            const scriptData = scriptResponse.data;

            // Get spi:autoscript
            const autoscript = scriptData["spi:autoscript"];
            if (autoscript) {
                scripts[scriptUrl] = autoscript;
                counter++;
            }
        }

        // Print all scripts
        for (const [key, value] of Object.entries(scripts)) {
            console.log(`Script: ${key}\n`);
            console.log(`${value}\n`);
        }

        console.log(`Total scripts: ${counter}`);
    } catch (error) {
        console.error("Error occurred:", error);
    }
}

main();
