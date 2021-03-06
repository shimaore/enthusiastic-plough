What is it?
===========

A script to split the CDR database into smaller databases

How does it work?
=================

    seem = require 'seem'

    run = seem (since,year) ->
      assert since?, 'since is required'
      assert year?, 'year is required'

It queries the CDR database in batches, and distributes each item into a target bin (database) for a given year.

      limit = cfg.limit ? 500

      console.log "#{pkg.name} #{pkg.version} starting for year #{year} at sequence #{since} for up to #{limit}."

      should_continue = true

      {body:{results}} = yield request
        .get "#{cfg.source}/_changes"
        .accept 'json'
        .timeout cfg.timeout
        .query
          limit: limit
          since: since
          include_docs: true

      assert results?, "Missing results"
      should_continue = results.length > 0
      assert results.length > 0, 'No results.'
      console.log "Splitting #{results.length} results."
      savers = {}
      for change in results
        do (change) ->
          {seq,doc} = change
          since = seq
          assert seq?, 'Missing seq'
          assert doc?, 'Missing doc'
          target_month = doc?.variables?.start_stamp?.substr(0,7)

Note: `target_month` might also be absent for deleted records (`change.deleted is true`).

          if target_month? and target_month.substr(0,4) is year
            target = savers[target_month] ?= new Saver target_month
            target.push doc
          else
            console.log "Skipped #{doc._id}, target_month = #{target_month}"

      for name,saver of savers
        yield saver.flush()

      {body:doc} = yield request
        .get since_url
        .accept 'json'
        .catch (error) ->
          console.error "#{since_id}: #{error}"
          body: _id: since_id
      doc.since = since
      doc.year = year
      yield request
        .put since_url
        .send doc
      yield run since, year if should_continue
      return

    pkg = require './package.json'
    path = require 'path'
    config_file = path.join (path.dirname module.filename), 'config.json'
    cfg = require config_file
    since_id = "#{pkg.name}.since"
    since_url = "#{cfg.source}/_local/#{since_id}"
    PouchDB = require 'pouchdb'
    request = require 'superagent'
    assert = require 'assert'

    class SaverError extends Error

    class Saver
      constructor: (@name) ->
        assert @name?, 'Missing @name'
        @db = new PouchDB "#{cfg.targets}/cdrs-#{@name}",
          ajax:
            timeout: cfg.timeout
        @queue = []

      push: (doc) ->
        assert doc?, 'Missing doc'
        delete doc._rev
        v = doc.variables
        doc._id = "#{v.start_stamp} #{v.ccnq_account} #{v.ccnq_from_e164} #{v.ccnq_to_e164} #{v.billsec}"
        @queue.push doc
        return

      flush: seem ->
        my_queue = @queue
        delete @queue
        console.log "#{@name}: Submitting #{my_queue.length} entries."
        responses = yield @db.bulkDocs my_queue
        count = 0
        for response in responses when not response.ok
          console.error "Failed for #{response}"
          count++
        throw new SaverError "Failed #{count} responses" if count > 0
        return

    main = seem ->
      {body:{since,year}} = yield request
        .get since_url
        .accept 'json'
        .catch (error) ->
          console.log "#{since_url}: #{error}"
          body: {}
      since ?= 1
      year ?= (new Date()).getFullYear().toString()
      yield run since, year
      return

    sleep = (timeout) ->
      new Promise (accept) ->
        setTimeout accept, timeout

    do seem ->
      while true
        yield main()
          .catch (error) ->
            console.error "Stopped with #{error}"
        console.error "Waiting 10s"
        yield sleep 10000
        return
