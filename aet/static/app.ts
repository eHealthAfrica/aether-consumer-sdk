window.Vue.config.productionTip = false;
var sockets = {};

var numSocket = new Rete.Socket('Number value');

function makeBasicUISchema(schema_){
  return [
    {
      component: 'input',
      model: 'firstName',
      fieldOptions: {
        class: ['form-control'],
        on: ['input'],
        attrs: {
          placeholder: 'Please enter your first name'
        }
      }
    }
  ]
}

function makeVueSchemaController(schema_){
  return {
    name: 'something',
    props: ['readonly', 'emitter', 'ikey', 'getData', 'putData'],
    template: '<vue-form-json-schema :model="model" :schema="schema" :ui-schema="uiSchema" v-on:change="onChange" v-on:state-change="onChangeState" v-on:validated="onValidated"></vue-form-json-schema>',
    data() {
      return {
        model: {},
        state: {},
        valid: false,
        schema: schema_,
        uiSchema: makeBasicUISchema(schema_)
      }
    },
    methods: {
      onChange(value) {
        console.log('onChange')
        // console.log(value)
        // this.state = value;
      },
      onChangeState(value) {
        console.log('onChangeState')
        // console.log(value)
        // this.state = value;
      },
      onValidated(value) {
        console.log('onValidated')
        // console.log(value)
        // this.valid = value;
      },
      change(e){
        console.log('change')
        console.log(e)
        // this.value = +e.target.value;
        // this.update();
      },
      update() {
        console.log('update')
        // if (this.ikey)
        //   this.putData(this.ikey, this.value)
        // this.emitter.trigger('process');
      }
    },
    mounted() {
      console.log('mounted')
      // this.value = this.getData(this.ikey);
    }
  }
}


class SchemaControl extends Rete.Control {

  constructor(emitter, key, schema_, readonly) {
    super(key);
    this.component = makeVueSchemaController(schema_)
    this.props = { emitter, ikey: key, readonly };
  }

  setValue(val) {
    // this.vueContext.value = val;
  }
}

var VueNumControl = {
  props: ['readonly', 'emitter', 'ikey', 'getData', 'putData'],
  template: '<input type="number" :readonly="readonly" :value="value" @input="change($event)" @dblclick.stop="" @pointerdown.stop="" @pointermove.stop=""/>',
  data() {
    return {
      value: 0,
    }
  },
  methods: {
    change(e){
      this.value = +e.target.value;
      this.update();
    },
    update() {
      if (this.ikey)
        this.putData(this.ikey, this.value)
      this.emitter.trigger('process');
    }
  },
  mounted() {
    this.value = this.getData(this.ikey);
  }
}

class NumControl extends Rete.Control {

  constructor(emitter, key, readonly) {
    super(key);
    this.component = VueNumControl;
    this.props = { emitter, ikey: key, readonly };
  }

  setValue(val) {
    this.vueContext.value = val;
  }
}

class NumComponent extends Rete.Component {

    constructor(){
        super("Number");
    }

    builder(node) {
        var out1 = new Rete.Output('num', "Number", numSocket);

        return node.addControl(new NumControl(this.editor, 'num')).addOutput(out1);
    }

    worker(node, inputs, outputs) {
        outputs['num'] = node.data.num;
    }
}

class PolyComponent extends Rete.Component{
    constructor(name, spec){
        super(name)
        this.name = name;
        this.spec = spec;
        if (!(name in sockets)){
            sockets[name] = new Rete.Socket(name)
        }
    }

    builder(node) {
        console.log(Object.keys(sockets))
        var inputs = this.spec["input"];
        var editor = this.editor;
        inputs.forEach(async function(name_){
                node.addInput(
                    new Rete.Input(name_, name_, sockets[name_])
                );
                console.log(name_);
        });
        var out1 = new Rete.Output(this.name, this.name, sockets[this.name]);
        var schema_ = {
        type: 'object',
        properties: {
          firstName: {
            type: 'string',
          }
        }
      }
        return node.addControl(new SchemaControl(this.editor, 'num', schema_)).addOutput(out1);
    }

    worker(node, inputs, outputs) {
        outputs['num'] = node.data.num;
    }
}

class AddComponent extends Rete.Component {
    constructor(){
        super("Add");
    }

    builder(node) {
        var inp1 = new Rete.Input('num',"Number", numSocket);
        var inp2 = new Rete.Input('num2', "Number2", numSocket);
        var out = new Rete.Output('num', "Number", numSocket);

        inp1.addControl(new NumControl(this.editor, 'num'))
        inp2.addControl(new NumControl(this.editor, 'num2'))

        return node
            .addInput(inp1)
            .addInput(inp2)
            .addControl(new NumControl(this.editor, 'preview', true))
            .addOutput(out);
    }

    worker(node, inputs, outputs) {
        var n1 = inputs['num'].length?inputs['num'][0]:node.data.num1;
        var n2 = inputs['num2'].length?inputs['num2'][0]:node.data.num2;
        var sum = n1 + n2;
        
        this.editor.nodes.find(n => n.id == node.id).controls.get('preview').setValue(sum);
        outputs['num'] = sum;
    }
}

(async () => {
    var container = document.querySelector('#rete');
    var editor = new Rete.NodeEditor('demo@0.1.0', container);
    editor.use(ConnectionPlugin.default);
    editor.use(VueRenderPlugin.default);    
    editor.use(ContextMenuPlugin.default);
    editor.use(AreaPlugin);
    editor.use(CommentPlugin.default);
    editor.use(HistoryPlugin);
    editor.use(ConnectionMasteryPlugin.default);


    // for name, spec yield new PolyComponent(name, spec)
    // var components = [new NumComponent(), new AddComponent()];
    var components = [new PolyComponent("a", {"input": ["b"]}), new PolyComponent("b", {"input": ["a"]})];

    var engine = new Rete.Engine('demo@0.1.0');
    
    components.map(c => {
        editor.register(c);
        engine.register(c);
    });

    // var n1 = await components[0].createNode({num: 2});
    // var n2 = await components[0].createNode({num: 0});
    // var add = await components[1].createNode();

    // n1.position = [80, 200];
    // n2.position = [80, 400];
    // add.position = [500, 240];
 

    // editor.addNode(n1);
    // editor.addNode(n2);
    // editor.addNode(add);

    // editor.connect(n1.outputs.get('num'), add.inputs.get('num'));
    // editor.connect(n2.outputs.get('num'), add.inputs.get('num2'));


    editor.on('process nodecreated noderemoved connectioncreated connectionremoved', async () => {
      console.log('process');
        await engine.abort();
        await engine.process(editor.toJSON());
    });

    editor.view.resize();
    AreaPlugin.zoomAt(editor);
    editor.trigger('process');
})();