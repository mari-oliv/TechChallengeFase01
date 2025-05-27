{
  "builds": [
    {
      "src": "api/index.py",
      "use": "@vercel/python",
      "config": {
        "maxLambdaSize": "50mb",
        "includeFiles": [
          "views//*",
          "routers//*",
          "data_extraction//*"
        ],
        "excludeFiles": [
          "env//*",
          "pycache//*",
          "assets//*"
        ]
      }
    }
  ],
  "routes": [
    {
      "src": "/(.*)",
      "dest": "api/index.py"
    }
  ]
}