const { TCP, constants } = process.binding("tcp_wrap");
const { EventEmitter } = require("events");
const { Socket: NTSocket } = require("net");

class Server extends EventEmitter {
  constructor() {
    super();
    this._handle = null;
    this._decoder = new TextDecoder();
  }

  listen(port, address, backlog) {
    const handle = new TCP(constants.SERVER);

    handle.bind(address || "0.0.0.0", port);
    const err = handle.listen(backlog || 511); // Backlog size

    if (err) {
      handle.close();
      throw new Error(`Error listening: ${err}`);
    }
    this._handle = handle;
    this._handle.onconnection = this.onconnection.bind(this);
  }

  onconnection(err, clientHandle) {
    if (err) {
      console.error("Error accepting connection:", err);
      return;
    }

    const socket = new NTSocket({ handle: clientHandle });

    socket.on("error", () => {
      socket.end()
    })

    socket._handle.onerror = (err) => {
      socket.end()
    };

    socket._handle.onread = (...args) => {
      try {
        const data = this._decoder.decode(args[0])
        this.emit("connection", socket, data);
      } catch (error) {
        socket.end();
      }
    }

    socket._handle.readStart();
  }

  close() {
    if (this._handle) {
      this._handle.close();
      this._handle = null;
    }
  }
}

module.exports = Server;