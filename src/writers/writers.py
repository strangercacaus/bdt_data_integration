import os

class DataWriter:
    def get_output_file_path(self, output_name=None, target_layer=None):
        """
        Get the output file path for a specific layer.
        
        Args:
            output_name (str, optional): Output file name. Defaults to self.stream.
            target_layer (str, optional): Target layer. Defaults to None.
            
        Returns:
            str: File path
        """
        if output_name is None:
            output_name = self.stream
        
        if target_layer is None:
            target_layer = self.target_layer
        
        # Usar caminho relativo em vez de absoluto
        base_dir = os.path.join(os.getcwd(), 'data')
        output_dir = os.path.join(base_dir, target_layer, self.source)
        
        # Criar diretórios se não existirem
        os.makedirs(output_dir, exist_ok=True)
        
        return os.path.join(output_dir, output_name) 